from __future__ import annotations

from datetime import datetime
from typing import Any

import asyncpg


class HealthMonitor:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def record_event(
        self,
        source: str,
        event_at: datetime,
        details: dict[str, Any] | None = None,
    ) -> None:
        async with self.pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO source_watermarks (
                    source,
                    last_event_at,
                    last_success_at,
                    details,
                    updated_at
                )
                VALUES ($1, $2, timezone('utc', now()), $3, timezone('utc', now()))
                ON CONFLICT (source) DO UPDATE
                SET
                    last_event_at = EXCLUDED.last_event_at,
                    last_success_at = EXCLUDED.last_success_at,
                    details = EXCLUDED.details,
                    updated_at = EXCLUDED.updated_at
                """,
                source,
                event_at,
                details or {},
            )

    async def snapshot(self) -> list[dict[str, Any]]:
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT source, last_event_at, last_success_at, details, updated_at
                FROM source_watermarks
                ORDER BY source
                """,
            )
        return [dict(row) for row in rows]
