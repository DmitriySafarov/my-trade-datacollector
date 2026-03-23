from __future__ import annotations

from collections.abc import Sequence

import asyncpg


class HyperliquidL2BookStore:
    _COLUMNS = (
        "time",
        "source",
        "coin",
        "bids",
        "asks",
        "snapshot_hash",
        "best_bid_price",
        "best_bid_size",
        "best_ask_price",
        "best_ask_size",
        "spread",
        "bid_notional",
        "ask_notional",
        "payload",
    )

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def write_many(self, records: Sequence[tuple[object, ...]]) -> None:
        if not records:
            return
        async with self._pool.acquire() as connection:
            await connection.copy_records_to_table(
                "hl_l2book",
                records=records,
                columns=self._COLUMNS,
            )
