"""Storage for hl_rest_funding with natural-key deduplication.

Uses INSERT ... ON CONFLICT DO NOTHING on the unique index
(source, time, coin) so duplicate funding records from
overlapping poll windows are silently skipped.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

import asyncpg

LOGGER = logging.getLogger(__name__)

_INSERT_SQL = """\
INSERT INTO hl_rest_funding (
    time, source, coin, funding, premium, mark_price, oracle_price, payload
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (source, time, coin) DO NOTHING
"""


class RestFundingStore:
    """Write parsed REST funding rows to hl_rest_funding."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def write_many(self, rows: Sequence[tuple[object, ...]]) -> None:
        """Write funding rows, silently skipping duplicates."""
        if not rows:
            return
        async with self._pool.acquire() as connection:
            await connection.executemany(_INSERT_SQL, rows)
        LOGGER.debug("hl_rest_funding_write count=%d", len(rows))
