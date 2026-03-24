"""Binance Futures aggTrade WebSocket collector.

Subscribes to ``<symbol>@aggTrade`` streams for ETH + BTC,
parses incoming messages, and writes to ``bn_agg_trades``
via a ``BatchWriter`` backed by COPY.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping, Sequence
from typing import Any

import asyncpg

from src.collectors.base import BaseCollector
from src.db.batch_writer import BatchWriter

from .agg_trade_parsing import (
    SOURCE_ID,
    BinanceAggTradeRecord,
    parse_binance_agg_trade,
)
from .agg_trade_storage import BinanceAggTradeStore
from .ws_manager import BinanceWsManager
from .ws_types import StreamConfig

LOGGER = logging.getLogger(__name__)
DEFAULT_SYMBOLS = ("ETHUSDT", "BTCUSDT")


class BinanceAggTradeCollector(BaseCollector):
    """Collects Binance Futures aggTrade data via combined WS stream."""

    name = "binance_agg_trades"
    source_ids = (SOURCE_ID,)

    def __init__(
        self,
        *,
        pool: asyncpg.Pool,
        count_limit: int,
        time_limit_seconds: float,
        symbols: Sequence[str] = DEFAULT_SYMBOLS,
        base_ws_url: str = "wss://fstream.binance.com/stream",
        reconnect_base_seconds: float = 1.0,
        reconnect_max_seconds: float = 30.0,
        reconnect_jitter_ratio: float = 0.2,
    ) -> None:
        self._symbols = tuple(s.upper() for s in symbols)
        if not self._symbols:
            raise ValueError("at least one symbol is required")
        store = BinanceAggTradeStore(pool)
        self._writer: BatchWriter[tuple[object, ...]] = BatchWriter(
            name="binance_agg_trades_writer",
            count_limit=count_limit,
            time_limit_seconds=time_limit_seconds,
            flush_callback=store.write_many,
        )
        streams = _build_stream_configs(self._symbols, self._writer)
        self._manager = BinanceWsManager(
            streams=streams,
            base_ws_url=base_ws_url,
            reconnect_base_seconds=reconnect_base_seconds,
            reconnect_max_seconds=reconnect_max_seconds,
            reconnect_jitter_ratio=reconnect_jitter_ratio,
        )
        self._run_task: asyncio.Task[None] | None = None
        self._stopped = asyncio.Event()
        self._stopped.set()

    async def start(self) -> None:
        self._stopped.clear()
        manager_task = asyncio.create_task(self._manager.run())
        writer_fail_task = asyncio.create_task(self._writer.wait_failure())
        error: BaseException | None = None
        try:
            done, _ = await asyncio.wait(
                {manager_task, writer_fail_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if writer_fail_task in done:
                error = _extract_error(writer_fail_task)
                await _stop_manager(self._manager, manager_task)
            else:
                # Manager exited — check if cleanly
                error = _extract_error(manager_task)
        except BaseException as caught:
            error = caught
        finally:
            writer_fail_task.cancel()
            await asyncio.gather(writer_fail_task, return_exceptions=True)
            if not manager_task.done():
                await _stop_manager(self._manager, manager_task)
            try:
                await self._writer.close()
            except Exception as close_err:
                error = error or close_err
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

    def health_snapshot(self) -> dict[str, Any]:
        snapshot = self._manager.health_snapshot()
        snapshot["name"] = self.name
        snapshot["source_ids"] = list(self.source_ids)
        snapshot["writer"] = self._writer.snapshot()
        return snapshot


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_stream_configs(
    symbols: tuple[str, ...],
    writer: BatchWriter[tuple[object, ...]],
) -> list[StreamConfig]:
    """Build one ``<symbol_lower>@aggTrade`` stream config per symbol."""
    configs: list[StreamConfig] = []
    for symbol in symbols:
        stream_name = f"{symbol.lower()}@aggTrade"

        async def handler(
            data: Mapping[str, object],
            *,
            _sym: str = symbol,
        ) -> None:
            await _handle_agg_trade(data, writer=writer, allowed_symbol=_sym)

        configs.append(StreamConfig(stream_name=stream_name, handler=handler))
    return configs


async def _handle_agg_trade(
    data: Mapping[str, object],
    *,
    writer: BatchWriter[tuple[object, ...]],
    allowed_symbol: str,
) -> None:
    """Parse a single aggTrade and enqueue for batch write."""
    try:
        record: BinanceAggTradeRecord = parse_binance_agg_trade(
            data,
            source=SOURCE_ID,
            allowed_symbols=(allowed_symbol,),
        )
    except (ValueError, OverflowError) as error:
        LOGGER.warning(
            "binance_agg_trade_invalid source=%s error=%s payload=%r",
            SOURCE_ID,
            error,
            data,
        )
        return
    await writer.add(record.as_copy_row())


async def _stop_manager(
    manager: BinanceWsManager,
    task: asyncio.Task[None],
) -> None:
    """Stop the WS manager and wait for its task, swallowing errors."""
    try:
        await manager.stop()
    except Exception:
        pass
    try:
        await asyncio.wait_for(task, timeout=5.0)
    except Exception:
        pass


def _extract_error(task: asyncio.Task[None]) -> BaseException | None:
    """Extract exception from a done task, if any."""
    if task.cancelled():
        return asyncio.CancelledError()
    return task.exception()
