from __future__ import annotations

from pathlib import Path

import pytest

from ._migration_upgrade import apply_repair_migrations, upgrade_db, utc


@pytest.mark.asyncio
async def test_quarantine_import_preserves_existing_archive_rows(
    tmp_path: Path,
) -> None:
    event_time = utc("2026-03-22T02:02:00+00:00")
    ingested_at = utc("2026-03-22T02:12:00+00:00")

    async with upgrade_db(tmp_path) as pool:
        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO bn_rest_delivery (
                    time,
                    source,
                    pair,
                    delivery_time,
                    delivery_price,
                    payload,
                    ingested_at
                )
                VALUES
                    ($1, 'bn_rest_delivery', 'BTCUSD', NULL, 43000.0, '{"row": 1}'::jsonb, $2)
                """,
                event_time,
                ingested_at,
            )
            await connection.execute(
                """
                CREATE TABLE bn_rest_delivery_legacy_nulls (
                    LIKE bn_rest_delivery INCLUDING DEFAULTS,
                    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    reason TEXT NOT NULL DEFAULT 'delivery_time_missing'
                )
                """,
            )
            await connection.execute(
                """
                INSERT INTO bn_rest_delivery_legacy_nulls (
                    time,
                    source,
                    pair,
                    delivery_time,
                    delivery_price,
                    payload,
                    ingested_at,
                    quarantined_at,
                    reason
                )
                VALUES
                    ($1, 'bn_rest_delivery', 'BTCUSD', NULL, 43000.0, '{"row": 1}'::jsonb, $2, $3, 'legacy')
                """,
                event_time,
                ingested_at,
                utc("2026-03-22T02:20:00+00:00"),
            )

        await apply_repair_migrations(pool, tmp_path, "repair_delivery_archive_dedup")

        async with pool.acquire() as connection:
            archive_rows = await connection.fetchrow(
                """
                SELECT
                    count(*) AS row_count,
                    count(*) FILTER (
                        WHERE source_chunk_oid IS NOT NULL AND source_ctid IS NOT NULL
                    ) AS live_archive_rows,
                    count(*) FILTER (
                        WHERE source_chunk_oid IS NULL AND source_ctid IS NULL
                    ) AS legacy_archive_rows
                FROM bn_rest_delivery_legacy_null_delivery_time
                WHERE pair = 'BTCUSD'
                  AND delivery_time IS NULL
                """,
            )

    assert dict(archive_rows) == {
        "row_count": 2,
        "live_archive_rows": 1,
        "legacy_archive_rows": 1,
    }
