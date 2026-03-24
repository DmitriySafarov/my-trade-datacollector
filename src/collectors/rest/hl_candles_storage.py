"""Storage for hl_rest_candles with natural-key deduplication.

Uses INSERT ... ON CONFLICT DO NOTHING on the unique index
(source, time, coin, interval) so duplicate candles from
overlapping poll windows are silently skipped.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

import asyncpg

LOGGER = logging.getLogger(__name__)

_INSERT_SQL = """\
INSERT INTO hl_rest_candles (
    time, source, coin, interval, open_time, close_time,
    open, high, low, close, volume, trades_count, payload
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
ON CONFLICT (source, time, coin, interval) DO NOTHING
"""


class RestCandleStore:
    """Write parsed REST candle rows to hl_rest_candles."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def write_many(self, rows: Sequence[tuple[object, ...]]) -> None:
        """Write candle rows, silently skipping duplicates."""
        if not rows:
            return
        async with self._pool.acquire() as connection:
            await connection.executemany(_INSERT_SQL, rows)
        LOGGER.debug("hl_rest_candles_write count=%d", len(rows))
