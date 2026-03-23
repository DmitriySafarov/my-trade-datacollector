from __future__ import annotations

import asyncio
from collections.abc import Sequence

import asyncpg

from src.collectors.base import BaseCollector
from src.db.batch_writer import BatchWriter

from .l2book_storage import HyperliquidL2BookStore
from .lifecycle_support import (
    close_writers,
    collect_task_errors,
    merge_error,
    stop_manager_task,
)
from .market_support import build_market_subscriptions
from .trade_storage import HyperliquidTradeStore
from .ws_manager import HyperliquidWsManager
from .ws_session import SdkHyperliquidWsSession, SessionFactory


TRADES_SOURCE_ID = "hl_ws_trades"
L2BOOK_SOURCE_ID = "hl_ws_l2book"
DEFAULT_COINS = ("ETH", "BTC")


class HyperliquidMarketCollector(BaseCollector):
    def __init__(
        self,
        *,
        base_url: str,
        pool: asyncpg.Pool,
        count_limit: int,
        time_limit_seconds: float,
        coins: Sequence[str] = DEFAULT_COINS,
        enable_trades: bool = True,
        enable_l2book: bool = True,
        reconnect_base_seconds: float = 1.0,
        reconnect_max_seconds: float = 30.0,
        reconnect_jitter_ratio: float = 0.2,
        session_factory: SessionFactory = SdkHyperliquidWsSession,
    ) -> None:
        self._coins = tuple(dict.fromkeys(coin.upper() for coin in coins))
        if not self._coins:
            raise ValueError("Hyperliquid market collector requires at least one coin")
        self.source_ids = tuple(
            source_id
            for enabled, source_id in (
                (enable_trades, TRADES_SOURCE_ID),
                (enable_l2book, L2BOOK_SOURCE_ID),
            )
            if enabled
        )
        if not self.source_ids:
            raise ValueError(
                "Hyperliquid market collector requires at least one source"
            )
        self.name = {
            (TRADES_SOURCE_ID,): "hyperliquid_trades",
            (L2BOOK_SOURCE_ID,): "hyperliquid_l2book",
        }.get(self.source_ids, "hyperliquid_market")
        self._trade_writer = (
            BatchWriter[tuple[object, ...]](
                name="hyperliquid_trades_writer",
                count_limit=count_limit,
                time_limit_seconds=time_limit_seconds,
                flush_callback=HyperliquidTradeStore(pool).write_many,
            )
            if enable_trades
            else None
        )
        self._l2book_writer = (
            BatchWriter[tuple[object, ...]](
                name="hyperliquid_l2book_writer",
                count_limit=count_limit,
                time_limit_seconds=time_limit_seconds,
                flush_callback=HyperliquidL2BookStore(pool).write_many,
            )
            if enable_l2book
            else None
        )
        self._writers = [
            writer
            for writer in (self._trade_writer, self._l2book_writer)
            if writer is not None
        ]
        self._stopped = asyncio.Event()
        self._stopped.set()
        self._manager = HyperliquidWsManager(
            base_url=base_url,
            subscriptions=build_market_subscriptions(
                coins=self._coins,
                trade_writer=self._trade_writer,
                l2book_writer=self._l2book_writer,
            ),
            reconnect_base_seconds=reconnect_base_seconds,
            reconnect_max_seconds=reconnect_max_seconds,
            reconnect_jitter_ratio=reconnect_jitter_ratio,
            session_factory=session_factory,
        )

    async def start(self) -> None:
        self._stopped.clear()
        manager_task = asyncio.create_task(self._manager.run())
        writer_failure_tasks = [
            asyncio.create_task(writer.wait_failure()) for writer in self._writers
        ]
        error: BaseException | None = None
        try:
            done, _ = await asyncio.wait(
                {manager_task, *writer_failure_tasks},
                return_when=asyncio.FIRST_COMPLETED,
            )
            failed_writer_tasks = [
                task for task in writer_failure_tasks if task in done
            ]
            if failed_writer_tasks:
                error = merge_error(
                    error, await collect_task_errors(failed_writer_tasks)
                )
                error = merge_error(
                    error,
                    await stop_manager_task(self._manager, manager_task),
                )
            else:
                await manager_task
            if error is None:
                for writer in self._writers:
                    if writer.failure is not None:
                        raise RuntimeError(
                            f"BatchWriter {writer.name} has failed"
                        ) from writer.failure
        except BaseException as caught:
            error = merge_error(error, caught)
        finally:
            for task in writer_failure_tasks:
                task.cancel()
            await asyncio.gather(*writer_failure_tasks, return_exceptions=True)
            try:
                if not manager_task.done():
                    error = merge_error(
                        error,
                        await stop_manager_task(self._manager, manager_task),
                    )
                error = merge_error(error, await close_writers(self._writers))
            finally:
                self._stopped.set()
        if error is not None:
            raise error

    async def stop(self) -> None:
        try:
            await self._manager.stop()
        finally:
            await self._stopped.wait()

    async def wait_ready(self) -> None:
        await self._manager.wait_ready()

    def health_snapshot(self) -> dict[str, object]:
        snapshot = self._manager.health_snapshot()
        snapshot["name"] = self.name
        snapshot["source_ids"] = list(self.source_ids)
        snapshot["source_health"] = snapshot["sources"]
        snapshot["writers"] = {
            source_id: writer.snapshot()
            for source_id, writer in (
                (TRADES_SOURCE_ID, self._trade_writer),
                (L2BOOK_SOURCE_ID, self._l2book_writer),
            )
            if writer is not None
        }
        return snapshot
