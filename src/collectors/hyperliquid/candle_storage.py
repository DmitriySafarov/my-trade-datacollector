from __future__ import annotations

from collections.abc import Sequence

import asyncpg


class HyperliquidCandleStore:
    _COLUMNS = (
        "time",
        "source",
        "coin",
        "interval",
        "open_time",
        "close_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "trades_count",
        "is_closed",
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
                "hl_candles",
                records=records,
                columns=self._COLUMNS,
            )
