from __future__ import annotations

import asyncpg

from src.db.batch_writer import BatchWriter

from .asset_ctx_storage import HyperliquidAssetCtxStore
from .candle_storage import HyperliquidCandleStore
from .l2book_storage import HyperliquidL2BookStore
from .trade_storage import HyperliquidTradeStore


def build_writer(
    *,
    name: str,
    count_limit: int,
    time_limit_seconds: float,
    flush_callback: object,
) -> BatchWriter[tuple[object, ...]]:
    return BatchWriter[tuple[object, ...]](
        name=name,
        count_limit=count_limit,
        time_limit_seconds=time_limit_seconds,
        flush_callback=flush_callback,
    )


def build_trade_writer(
    pool: asyncpg.Pool, count_limit: int, time_limit_seconds: float
) -> BatchWriter[tuple[object, ...]]:
    return build_writer(
        name="hyperliquid_trades_writer",
        count_limit=count_limit,
        time_limit_seconds=time_limit_seconds,
        flush_callback=HyperliquidTradeStore(pool).write_many,
    )


def build_l2book_writer(
    pool: asyncpg.Pool, count_limit: int, time_limit_seconds: float
) -> BatchWriter[tuple[object, ...]]:
    return build_writer(
        name="hyperliquid_l2book_writer",
        count_limit=count_limit,
        time_limit_seconds=time_limit_seconds,
        flush_callback=HyperliquidL2BookStore(pool).write_many,
    )


def build_asset_ctx_writer(
    pool: asyncpg.Pool, count_limit: int, time_limit_seconds: float
) -> BatchWriter[tuple[object, ...]]:
    return build_writer(
        name="hyperliquid_asset_ctx_writer",
        count_limit=count_limit,
        time_limit_seconds=time_limit_seconds,
        flush_callback=HyperliquidAssetCtxStore(pool).write_many,
    )


def build_candle_writer(
    pool: asyncpg.Pool, count_limit: int, time_limit_seconds: float
) -> BatchWriter[tuple[object, ...]]:
    return build_writer(
        name="hyperliquid_candles_writer",
        count_limit=count_limit,
        time_limit_seconds=time_limit_seconds,
        flush_callback=HyperliquidCandleStore(pool).write_many,
    )
