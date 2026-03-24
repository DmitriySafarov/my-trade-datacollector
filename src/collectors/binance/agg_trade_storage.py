"""COPY-based storage for Binance aggTrade records into ``bn_agg_trades``."""

from __future__ import annotations

from collections.abc import Sequence

import asyncpg


class BinanceAggTradeStore:
    """Writes parsed aggTrade rows to the ``bn_agg_trades`` hypertable via COPY."""

    _COLUMNS = (
        "time",
        "source",
        "symbol",
        "trade_id",
        "agg_trade_id",
        "first_trade_id",
        "last_trade_id",
        "price",
        "qty",
        "is_buyer_maker",
        "is_best_match",
        "payload",
    )

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def write_many(self, records: Sequence[tuple[object, ...]]) -> None:
        if not records:
            return
        async with self._pool.acquire() as connection:
            await connection.copy_records_to_table(
                "bn_agg_trades",
                records=records,
                columns=self._COLUMNS,
            )
