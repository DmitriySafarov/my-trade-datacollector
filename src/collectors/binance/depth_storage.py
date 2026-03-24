"""COPY-based storage for Binance depth records into ``bn_depth``."""

from __future__ import annotations

from collections.abc import Sequence

import asyncpg


class BinanceDepthStore:
    """Writes parsed depth rows to the ``bn_depth`` hypertable via COPY."""

    _COLUMNS = (
        "time",
        "source",
        "symbol",
        "first_update_id",
        "final_update_id",
        "last_update_id",
        "bids",
        "asks",
        "snapshot_hash",
        "best_bid_price",
        "best_ask_price",
        "payload",
    )

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def write_many(self, records: Sequence[tuple[object, ...]]) -> None:
        if not records:
            return
        async with self._pool.acquire() as connection:
            await connection.copy_records_to_table(
                "bn_depth",
                records=records,
                columns=self._COLUMNS,
            )
