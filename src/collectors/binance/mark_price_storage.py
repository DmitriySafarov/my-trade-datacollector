"""COPY-based storage for Binance markPrice records into ``bn_mark_price``."""

from __future__ import annotations

from collections.abc import Sequence

import asyncpg


class BinanceMarkPriceStore:
    """Writes parsed markPrice rows to the ``bn_mark_price`` hypertable via COPY."""

    _COLUMNS = (
        "time",
        "source",
        "symbol",
        "mark_price",
        "index_price",
        "estimated_settle_price",
        "premium",
        "funding_rate",
        "next_funding_time",
        "event_hash",
        "payload",
    )

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def write_many(self, records: Sequence[tuple[object, ...]]) -> None:
        if not records:
            return
        async with self._pool.acquire() as connection:
            await connection.copy_records_to_table(
                "bn_mark_price",
                records=records,
                columns=self._COLUMNS,
            )
