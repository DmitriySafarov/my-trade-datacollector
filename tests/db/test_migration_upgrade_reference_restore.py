from __future__ import annotations

from pathlib import Path

import asyncpg
import pytest

from src.db.migrations import run_migrations

from ._migration_upgrade import (
    compress_tables,
    upgrade_db,
    utc,
    write_migration_subset,
)


@pytest.mark.asyncio
async def test_forward_repair_restores_deleted_delivery_history_rows(
    tmp_path: Path,
) -> None:
    first_delivery = utc("2026-03-28T08:00:00+00:00")

    async with upgrade_db(tmp_path) as pool:
        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO bn_rest_delivery (
                    time, source, pair, delivery_time, delivery_price, ingested_at
                )
                VALUES
                    ($1, 'bn_rest_delivery', 'BTCUSD', $2, 43000.0, $3),
                    ($4, 'bn_rest_delivery', 'BTCUSD', NULL, 43000.0, $5)
                """,
                utc("2026-03-22T02:00:00+00:00"),
                first_delivery,
                utc("2026-03-22T02:10:00+00:00"),
                utc("2026-03-22T02:02:00+00:00"),
                utc("2026-03-22T02:12:00+00:00"),
            )

        pre_restore_dir = write_migration_subset(
            tmp_path,
            "repair_delivery_pre_0023",
            {
                path.name
                for path in (Path(__file__).resolve().parents[2] / "migrations").glob(
                    "*.sql"
                )
                if "0012_bronze_dedup_repairs.sql" <= path.name
                and path.name < "0023_delivery_null_bronze_restore.sql"
            },
        )
        await run_migrations(pool, pre_restore_dir)
        async with pool.acquire() as connection:
            await connection.execute(
                """
                DELETE FROM bn_rest_delivery
                WHERE pair = 'BTCUSD' AND delivery_time IS NULL
                """,
            )
            await connection.execute(
                """
                INSERT INTO bn_rest_delivery_legacy_null_delivery_time (
                    time,
                    source,
                    pair,
                    delivery_time,
                    delivery_price,
                    payload,
                    ingested_at,
                    quarantined_at,
                    quarantine_reason
                )
                VALUES (
                    $1,
                    'bn_rest_delivery',
                    'BTCUSD',
                    NULL,
                    43000.0,
                    '{}'::jsonb,
                    $2,
                    $3,
                    'duplicate'
                )
                """,
                utc("2026-03-22T02:02:00+00:00"),
                utc("2026-03-22T02:12:00+00:00"),
                utc("2026-03-22T02:30:00+00:00"),
            )
        await compress_tables(pool, "bn_rest_delivery")

        restore_dir = write_migration_subset(
            tmp_path,
            "repair_delivery_0023",
            {"0023_delivery_null_bronze_restore.sql"},
        )
        await run_migrations(pool, restore_dir)

        async with pool.acquire() as connection:
            counts = await connection.fetchrow(
                """
                SELECT
                    (SELECT count(*) FROM bn_rest_delivery WHERE pair = 'BTCUSD' AND delivery_time IS NULL) AS live_null_rows,
                    (SELECT count(*) FROM bn_rest_delivery_legacy_null_delivery_time WHERE pair = 'BTCUSD' AND delivery_time IS NULL) AS legacy_null_rows,
                    (SELECT count(*) FROM bn_rest_delivery_registry WHERE pair = 'BTCUSD') AS registry_rows,
                    (
                        SELECT count(*)
                        FROM pg_constraint
                        WHERE conrelid = 'bn_rest_delivery'::regclass
                          AND conname = 'bn_rest_delivery_delivery_time_required'
                          AND convalidated = false
                    ) AS check_rows
                """,
            )
            with pytest.raises(asyncpg.CheckViolationError):
                await connection.execute(
                    """
                    INSERT INTO bn_rest_delivery (time, source, pair)
                    VALUES ($1, 'bn_rest_delivery', 'BTCUSD')
                    """,
                    utc("2026-03-22T02:05:00+00:00"),
                )

    assert dict(counts) == {
        "live_null_rows": 1,
        "legacy_null_rows": 2,
        "registry_rows": 1,
        "check_rows": 1,
    }


@pytest.mark.asyncio
async def test_forward_repair_restores_multiple_quarantined_rows_for_same_poll(
    tmp_path: Path,
) -> None:
    event_time = utc("2026-03-22T02:02:00+00:00")

    async with upgrade_db(tmp_path) as pool:
        pre_restore_dir = write_migration_subset(
            tmp_path,
            "repair_delivery_pre_0023_multi",
            {
                path.name
                for path in (Path(__file__).resolve().parents[2] / "migrations").glob(
                    "*.sql"
                )
                if "0012_bronze_dedup_repairs.sql" <= path.name
                and path.name < "0023_delivery_null_bronze_restore.sql"
            },
        )
        await run_migrations(pool, pre_restore_dir)
        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO bn_rest_delivery_legacy_null_delivery_time (
                    time, source, pair, delivery_time, delivery_price, payload,
                    ingested_at, quarantined_at, quarantine_reason
                )
                VALUES
                    ($1, 'bn_rest_delivery', 'BTCUSD', NULL, 43000.0, '{"row": 1}'::jsonb, $2, $3, 'legacy'),
                    ($1, 'bn_rest_delivery', 'BTCUSD', NULL, 43100.0, '{"row": 2}'::jsonb, $4, $5, 'legacy')
                """,
                event_time,
                utc("2026-03-22T02:12:00+00:00"),
                utc("2026-03-22T02:30:00+00:00"),
                utc("2026-03-22T02:13:00+00:00"),
                utc("2026-03-22T02:31:00+00:00"),
            )

        restore_dir = write_migration_subset(
            tmp_path,
            "repair_delivery_0023_multi",
            {"0023_delivery_null_bronze_restore.sql"},
        )
        await run_migrations(pool, restore_dir)

        async with pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT delivery_price, payload
                FROM bn_rest_delivery
                WHERE source = 'bn_rest_delivery'
                  AND pair = 'BTCUSD'
                  AND time = $1
                  AND delivery_time IS NULL
                ORDER BY delivery_price
                """,
                event_time,
            )

    assert [row["delivery_price"] for row in rows] == [43000.0, 43100.0]
