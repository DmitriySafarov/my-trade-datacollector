"""Binance Futures markPrice @1s WebSocket collector.

Subscribes to ``<symbol>@markPrice@1s`` streams for ETH + BTC,
parses incoming messages, and writes to ``bn_mark_price``
via a ``BatchWriter`` backed by COPY.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence

import asyncpg

from src.db.batch_writer import BatchWriter

from .binance_ws_collector_base import BinanceWsCollectorBase
from .mark_price_parsing import (
    SOURCE_ID,
    BinanceMarkPriceRecord,
    parse_binance_mark_price,
)
from .mark_price_storage import BinanceMarkPriceStore
from .ws_types import StreamConfig

LOGGER = logging.getLogger(__name__)
DEFAULT_SYMBOLS = ("ETHUSDT", "BTCUSDT")


class BinanceMarkPriceCollector(BinanceWsCollectorBase):
    """Collects Binance Futures markPrice @1s data via combined WS stream."""

    name = "binance_mark_price"
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
        symbols_upper = tuple(s.upper() for s in symbols)
        if not symbols_upper:
            raise ValueError("at least one symbol is required")
        store = BinanceMarkPriceStore(pool)
        writer: BatchWriter[tuple[object, ...]] = BatchWriter(
            name="binance_mark_price_writer",
            count_limit=count_limit,
            time_limit_seconds=time_limit_seconds,
            flush_callback=store.write_many,
        )
        streams = _build_stream_configs(symbols_upper, writer)
        super().__init__(
            writer=writer,
            streams=streams,
            base_ws_url=base_ws_url,
            reconnect_base_seconds=reconnect_base_seconds,
            reconnect_max_seconds=reconnect_max_seconds,
            reconnect_jitter_ratio=reconnect_jitter_ratio,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_stream_configs(
    symbols: tuple[str, ...],
    writer: BatchWriter[tuple[object, ...]],
) -> list[StreamConfig]:
    """Build one ``<symbol_lower>@markPrice@1s`` stream config per symbol."""
    configs: list[StreamConfig] = []
    for symbol in symbols:
        stream_name = f"{symbol.lower()}@markPrice@1s"

        async def handler(
            data: Mapping[str, object],
            *,
            _sym: str = symbol,
        ) -> None:
            await _handle_mark_price(data, writer=writer, allowed_symbol=_sym)

        configs.append(StreamConfig(stream_name=stream_name, handler=handler))
    return configs


async def _handle_mark_price(
    data: Mapping[str, object],
    *,
    writer: BatchWriter[tuple[object, ...]],
    allowed_symbol: str,
) -> None:
    """Parse a single markPriceUpdate and enqueue for batch write."""
    try:
        record: BinanceMarkPriceRecord = parse_binance_mark_price(
            data,
            source=SOURCE_ID,
            allowed_symbols=(allowed_symbol,),
        )
    except (ValueError, OverflowError) as error:
        LOGGER.warning(
            "binance_mark_price_invalid source=%s error=%s payload=%r",
            SOURCE_ID,
            error,
            data,
        )
        return
    await writer.add(record.as_copy_row())
