from __future__ import annotations

import json
from pathlib import Path

import asyncpg
import pytest

from ._migration_upgrade import (
    apply_repair_migrations,
    compress_tables,
    upgrade_db,
    utc,
)


@pytest.mark.asyncio
async def test_upgrade_repairs_preserve_delivery_history_and_rebuild_registry(
    tmp_path: Path,
) -> None:
    first_delivery = utc("2026-03-28T08:00:00+00:00")
    second_delivery = utc("2026-06-26T08:00:00+00:00")

    async with upgrade_db(tmp_path) as pool:
        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO bn_rest_delivery (
                    time, source, pair, delivery_time, delivery_price, ingested_at
                )
                VALUES
                    ($1, 'bn_rest_delivery', 'BTCUSD', $2, 43000.0, $3),
                    ($4, 'bn_rest_delivery', 'BTCUSD', $2, 43000.0, $5),
                    ($6, 'bn_rest_delivery', 'BTCUSD', NULL, 43000.0, $7)
                """,
                utc("2026-03-22T02:00:00+00:00"),
                first_delivery,
                utc("2026-03-22T02:10:00+00:00"),
                utc("2026-03-22T02:01:00+00:00"),
                utc("2026-03-22T02:11:00+00:00"),
                utc("2026-03-22T02:02:00+00:00"),
                utc("2026-03-22T02:12:00+00:00"),
            )

        await compress_tables(pool, "bn_rest_delivery")
        await apply_repair_migrations(pool, tmp_path, "repair_delivery")

        async with pool.acquire() as connection:
            counts = await connection.fetchrow(
                """
                SELECT
                    (SELECT count(*) FROM bn_rest_delivery WHERE pair = 'BTCUSD' AND delivery_time = $1) AS duplicate_rows,
                    (SELECT count(*) FROM bn_rest_delivery WHERE pair = 'BTCUSD' AND delivery_time IS NULL) AS live_null_rows,
                    (SELECT count(*) FROM bn_rest_delivery_legacy_null_delivery_time WHERE pair = 'BTCUSD' AND delivery_time IS NULL) AS legacy_null_rows,
                    (SELECT count(*) FROM bn_rest_delivery_registry WHERE pair = 'BTCUSD' AND delivery_time = $1) AS registry_rows,
                    (
                        SELECT count(*)
                        FROM pg_constraint
                        WHERE conrelid = 'bn_rest_delivery'::regclass
                          AND conname = 'bn_rest_delivery_delivery_time_required'
                          AND convalidated = false
                    ) AS check_rows
                """,
                first_delivery,
            )

            with pytest.raises(asyncpg.CheckViolationError):
                await connection.execute(
                    """
                    INSERT INTO bn_rest_delivery (time, source, pair)
                    VALUES ($1, 'bn_rest_delivery', 'BTCUSD')
                    """,
                    utc("2026-03-22T02:05:00+00:00"),
                )

            await connection.execute(
                """
                INSERT INTO bn_rest_delivery (
                    time, source, pair, delivery_time, delivery_price
                )
                VALUES ($1, 'bn_rest_delivery', 'BTCUSD', $2, 43000.0)
                ON CONFLICT DO NOTHING
                """,
                utc("2026-03-22T02:06:00+00:00"),
                first_delivery,
            )
            await connection.execute(
                """
                INSERT INTO bn_rest_delivery (
                    time, source, pair, delivery_time, delivery_price
                )
                VALUES ($1, 'bn_rest_delivery', 'BTCUSD', $2, 43100.0)
                ON CONFLICT DO NOTHING
                """,
                utc("2026-03-22T02:07:00+00:00"),
                second_delivery,
            )

            final_counts = await connection.fetchrow(
                """
                SELECT
                    (SELECT count(*) FROM bn_rest_delivery WHERE pair = 'BTCUSD' AND delivery_time = $1) AS preserved_duplicate_rows,
                    (SELECT count(*) FROM bn_rest_delivery_registry WHERE pair = 'BTCUSD') AS total_registry_rows
                """,
                first_delivery,
            )

    assert dict(counts) == {
        "duplicate_rows": 2,
        "live_null_rows": 1,
        "legacy_null_rows": 1,
        "registry_rows": 1,
        "check_rows": 1,
    }
    assert dict(final_counts) == {
        "preserved_duplicate_rows": 3,
        "total_registry_rows": 2,
    }


@pytest.mark.asyncio
async def test_upgrade_repairs_preserve_all_legacy_delivery_quarantine_rows(
    tmp_path: Path,
) -> None:
    event_time = utc("2026-03-22T02:02:00+00:00")
    ingested_at = utc("2026-03-22T02:12:00+00:00")

    async with upgrade_db(tmp_path) as pool:
        async with pool.acquire() as connection:
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
                    ($1, 'bn_rest_delivery', 'BTCUSD', NULL, 43000.0, '{"row": 1}'::jsonb, $2, $3, 'legacy'),
                    ($1, 'bn_rest_delivery', 'BTCUSD', NULL, 43100.0, '{"row": 2}'::jsonb, $2, $4, 'legacy')
                """,
                event_time,
                ingested_at,
                utc("2026-03-22T02:20:00+00:00"),
                utc("2026-03-22T02:21:00+00:00"),
            )

        await apply_repair_migrations(pool, tmp_path, "repair_delivery_legacy")

        async with pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT delivery_price, payload, source_chunk_oid, source_ctid
                FROM bn_rest_delivery_legacy_null_delivery_time
                WHERE pair = 'BTCUSD'
                ORDER BY delivery_price
                """,
            )
            legacy_table = await connection.fetchval(
                "SELECT to_regclass('bn_rest_delivery_legacy_nulls')",
            )

    assert [row["delivery_price"] for row in rows] == [43000.0, 43100.0]
    assert [json.loads(row["payload"]) for row in rows] == [{"row": 1}, {"row": 2}]
    assert all(row["source_chunk_oid"] is None for row in rows)
    assert all(row["source_ctid"] is None for row in rows)
    assert legacy_table is None
