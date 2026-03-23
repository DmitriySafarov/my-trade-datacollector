from __future__ import annotations

from collections.abc import Sequence

import asyncpg


class HyperliquidTradeStore:
    _COLUMNS = (
        "time",
        "source",
        "coin",
        "side",
        "price",
        "size",
        "hash",
        "tid",
        "users",
        "payload",
    )

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def write_many(self, records: Sequence[tuple[object, ...]]) -> None:
        if not records:
            return
        async with self._pool.acquire() as connection:
            await connection.copy_records_to_table(
                "hl_trades",
                records=records,
                columns=self._COLUMNS,
            )
