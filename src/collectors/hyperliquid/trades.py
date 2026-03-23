from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping, Sequence

import asyncpg

from src.collectors.base import BaseCollector
from src.db.batch_writer import BatchWriter

from .trade_parsing import parse_hyperliquid_trade
from .trade_storage import HyperliquidTradeStore
from .ws_manager import HyperliquidWsManager
from .ws_session import SdkHyperliquidWsSession, SessionFactory
from .ws_subscription import HyperliquidWsSubscription


SOURCE_ID = "hl_ws_trades"
DEFAULT_COINS = ("ETH", "BTC")
LOGGER = logging.getLogger(__name__)


class HyperliquidTradesCollector(BaseCollector):
    name = "hyperliquid_trades"
    source_ids = (SOURCE_ID,)

    def __init__(
        self,
        *,
        base_url: str,
        pool: asyncpg.Pool,
        count_limit: int,
        time_limit_seconds: float,
        coins: Sequence[str] = DEFAULT_COINS,
        reconnect_base_seconds: float = 1.0,
        reconnect_max_seconds: float = 30.0,
        reconnect_jitter_ratio: float = 0.2,
        session_factory: SessionFactory = SdkHyperliquidWsSession,
    ) -> None:
        self._coins = tuple(dict.fromkeys(coin.upper() for coin in coins))
        if not self._coins:
            raise ValueError("Hyperliquid trades collector requires at least one coin")
        self._writer = BatchWriter[tuple[object, ...]](
            name=f"{self.name}_writer",
            count_limit=count_limit,
            time_limit_seconds=time_limit_seconds,
            flush_callback=HyperliquidTradeStore(pool).write_many,
        )
        self._stopped = asyncio.Event()
        self._stopped.set()
        self._manager = HyperliquidWsManager(
            base_url=base_url,
            subscriptions=[
                HyperliquidWsSubscription(
                    name=f"trades:{coin.lower()}",
                    subscription={"type": "trades", "coin": coin},
                    handler=self._handle_message,
                )
                for coin in self._coins
            ],
            reconnect_base_seconds=reconnect_base_seconds,
            reconnect_max_seconds=reconnect_max_seconds,
            reconnect_jitter_ratio=reconnect_jitter_ratio,
            session_factory=session_factory,
        )

    async def start(self) -> None:
        self._stopped.clear()
        manager_task = asyncio.create_task(self._manager.run())
        writer_failure_task = asyncio.create_task(self._writer.wait_failure())
        try:
            done, _ = await asyncio.wait(
                {manager_task, writer_failure_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if writer_failure_task in done:
                await self._manager.stop()
                await manager_task
                await writer_failure_task
            await manager_task
            if self._writer.failure is not None:
                raise RuntimeError(
                    f"BatchWriter {self._writer.name} has failed"
                ) from self._writer.failure
        finally:
            writer_failure_task.cancel()
            await asyncio.gather(writer_failure_task, return_exceptions=True)
            try:
                await self._writer.close()
            finally:
                self._stopped.set()

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
        snapshot["writer"] = self._writer.snapshot()
        return snapshot

    async def _handle_message(self, message: object) -> None:
        data = self._extract_trades(message)
        if data is None:
            return
        records = self._parse_records(data)
        if records:
            await self._writer.add_many(records)

    def _extract_trades(self, message: object) -> list[object] | None:
        try:
            return self._require_trade_batch(message)
        except ValueError as error:
            LOGGER.warning(
                "hyperliquid_trade_message_invalid source=%s error=%s payload=%r",
                SOURCE_ID,
                error,
                message,
            )
            return None

    def _parse_records(self, trades: Sequence[object]) -> list[tuple[object, ...]]:
        records: list[tuple[object, ...]] = []
        for index, trade in enumerate(trades):
            try:
                record = parse_hyperliquid_trade(
                    trade,
                    source=SOURCE_ID,
                    allowed_coins=self._coins,
                )
            except (OSError, OverflowError, ValueError) as error:
                LOGGER.warning(
                    "hyperliquid_trade_payload_invalid source=%s index=%s error=%s payload=%r",
                    SOURCE_ID,
                    index,
                    error,
                    trade,
                )
                continue
            records.append(record.as_copy_row())
        return records

    def _require_trade_batch(self, message: object) -> list[object]:
        if not isinstance(message, Mapping):
            raise ValueError("Hyperliquid trades message must be a mapping")
        if message.get("channel") != "trades":
            raise ValueError("Unexpected Hyperliquid trades channel")
        data = message.get("data")
        if not isinstance(data, list):
            raise ValueError("Hyperliquid trades payload must be a list")
        return data
