from __future__ import annotations

import json
from datetime import datetime, timezone
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
        event_at = _normalize_event_at(event_at)
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
                VALUES ($1, $2, CURRENT_TIMESTAMP, $3::jsonb, CURRENT_TIMESTAMP)
                ON CONFLICT (source) DO UPDATE
                SET
                    last_event_at = COALESCE(
                        GREATEST(
                            source_watermarks.last_event_at,
                            EXCLUDED.last_event_at
                        ),
                        source_watermarks.last_event_at,
                        EXCLUDED.last_event_at
                    ),
                    last_success_at = CASE
                        WHEN source_watermarks.last_success_at IS NULL
                            THEN CURRENT_TIMESTAMP
                        ELSE GREATEST(
                            source_watermarks.last_success_at,
                            CURRENT_TIMESTAMP
                        )
                    END,
                    details = CASE
                        WHEN source_watermarks.last_event_at IS NULL THEN EXCLUDED.details
                        WHEN EXCLUDED.last_event_at > source_watermarks.last_event_at
                            THEN EXCLUDED.details
                        ELSE source_watermarks.details
                    END,
                    updated_at = CASE
                        WHEN source_watermarks.updated_at IS NULL
                            THEN CURRENT_TIMESTAMP
                        ELSE GREATEST(
                            source_watermarks.updated_at,
                            CURRENT_TIMESTAMP
                        )
                    END
                """,
                source,
                event_at,
                json.dumps(details or {}),
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

        snapshots: list[dict[str, Any]] = []
        for row in rows:
            row_dict = dict(row)
            if isinstance(row_dict["details"], str):
                row_dict["details"] = json.loads(row_dict["details"])
            snapshots.append(row_dict)
        return snapshots


def _normalize_event_at(event_at: datetime) -> datetime:
    if event_at.tzinfo is None or event_at.utcoffset() is None:
        raise ValueError("event_at must be timezone-aware")
    return event_at.astimezone(timezone.utc)
