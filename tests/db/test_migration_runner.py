from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from src.db.migrations import MIGRATION_LOCK_ID, run_migrations
from src.db.pool import close_pool, create_pool


ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.asyncio
async def test_migrations_are_idempotent(migrated_db: dict[str, object]) -> None:
    pool = migrated_db["pool"]
    rerun = await run_migrations(pool, ROOT / "migrations")
    assert rerun == []


@pytest.mark.asyncio
async def test_run_migrations_is_serialized(
    migrated_db: dict[str, object],
    tmp_path: Path,
) -> None:
    config = migrated_db["config"]
    migration_dir = tmp_path / "migrations"
    migration_dir.mkdir()
    versions = ["9000_lock_probe.sql", "9001_lock_insert.sql"]
    (migration_dir / versions[0]).write_text(
        """
        SELECT pg_sleep(0.2);
        CREATE TABLE IF NOT EXISTS migration_lock_probe (
            id INTEGER PRIMARY KEY,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """,
        encoding="utf-8",
    )
    (migration_dir / versions[1]).write_text(
        "INSERT INTO migration_lock_probe (id) VALUES (1) ON CONFLICT DO NOTHING;\n",
        encoding="utf-8",
    )

    pool_a = None
    pool_b = None
    try:
        pool_a = await create_pool(config)
        pool_b = await create_pool(config)
        first, second = await asyncio.gather(
            run_migrations(pool_a, migration_dir),
            run_migrations(pool_b, migration_dir),
        )
    finally:
        if pool_b is not None:
            await close_pool(pool_b)
        if pool_a is not None:
            await close_pool(pool_a)

    assert sorted(first + second) == versions

    async with migrated_db["pool"].acquire() as connection:
        applied_count = await connection.fetchval(
            """
            SELECT count(*)
            FROM schema_migrations
            WHERE version = ANY($1::text[])
            """,
            versions,
        )
        row_count = await connection.fetchval(
            "SELECT count(*) FROM migration_lock_probe",
        )

    assert applied_count == len(versions)
    assert row_count == 1


@pytest.mark.asyncio
async def test_run_migrations_stops_while_waiting_on_lock(
    migrated_db: dict[str, object],
    tmp_path: Path,
) -> None:
    config = migrated_db["config"]
    migration_dir = tmp_path / "migrations"
    migration_dir.mkdir()
    version = "9002_lock_wait.sql"
    (migration_dir / version).write_text("SELECT 1;\n", encoding="utf-8")
    attempts = {"count": 0}

    def stop_requested() -> bool:
        attempts["count"] += 1
        return attempts["count"] > 1

    lock_pool = await create_pool(config)
    try:
        async with lock_pool.acquire() as connection:
            await connection.execute("SELECT pg_advisory_lock($1)", MIGRATION_LOCK_ID)
            try:
                rerun = await run_migrations(
                    migrated_db["pool"],
                    migration_dir,
                    stop_requested=stop_requested,
                    lock_retry_seconds=0.01,
                )
            finally:
                await connection.execute(
                    "SELECT pg_advisory_unlock($1)",
                    MIGRATION_LOCK_ID,
                )
    finally:
        await close_pool(lock_pool)

    async with migrated_db["pool"].acquire() as connection:
        applied_count = await connection.fetchval(
            """
            SELECT count(*)
            FROM schema_migrations
            WHERE version = $1
            """,
            version,
        )

    assert rerun == []
    assert attempts["count"] > 1
    assert applied_count == 0
