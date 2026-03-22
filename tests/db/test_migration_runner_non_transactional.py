from __future__ import annotations

import asyncio
from pathlib import Path
from time import monotonic

import asyncpg
import pytest

from src.db.migrations import run_migrations


@pytest.mark.asyncio
async def test_non_transactional_migration_can_resume_after_fix_in_maintenance_mode(
    migrated_db: dict[str, object],
    tmp_path: Path,
) -> None:
    migration_dir = tmp_path / "migrations"
    migration_dir.mkdir()
    version = "9010_non_transactional_probe.sql"
    probe_sql = """
    -- migrate: no-transaction
    CREATE TABLE IF NOT EXISTS non_transactional_probe (
        id INTEGER PRIMARY KEY
    );
    INSERT INTO non_transactional_probe (id)
    SELECT id FROM non_transactional_resume_seed;
    """
    (migration_dir / version).write_text(probe_sql, encoding="utf-8")

    with pytest.raises(asyncpg.UndefinedTableError):
        await run_migrations(migrated_db["pool"], migration_dir)

    async with migrated_db["pool"].acquire() as connection:
        relation = await connection.fetchval(
            "SELECT to_regclass('non_transactional_probe')",
        )
        row_count = await connection.fetchval(
            "SELECT count(*) FROM non_transactional_probe",
        )
        status = await connection.fetchval(
            """
            SELECT status
            FROM schema_migrations
            WHERE version = $1
            """,
            version,
        )

    assert relation == "non_transactional_probe"
    assert row_count == 0
    assert status == "failed"

    async with migrated_db["pool"].acquire() as connection:
        await connection.execute(
            """
            CREATE TABLE non_transactional_resume_seed (
                id INTEGER PRIMARY KEY
            )
            """,
        )
        await connection.execute(
            """
            INSERT INTO non_transactional_resume_seed (id)
            VALUES (1)
            """,
        )
    rerun = await run_migrations(migrated_db["pool"], migration_dir)

    async with migrated_db["pool"].acquire() as connection:
        status = await connection.fetchval(
            """
            SELECT status
            FROM schema_migrations
            WHERE version = $1
            """,
            version,
        )
        row_count = await connection.fetchval(
            "SELECT count(*) FROM non_transactional_probe",
        )

    assert rerun == [version]
    assert status == "applied"
    assert row_count == 1


@pytest.mark.asyncio
async def test_non_transactional_migration_finishes_after_stop_requested(
    migrated_db: dict[str, object],
    tmp_path: Path,
) -> None:
    migration_dir = tmp_path / "migrations"
    migration_dir.mkdir()
    first_version = "9011_non_transactional_stop_probe.sql"
    second_version = "9012_non_transactional_stop_tail.sql"
    (migration_dir / first_version).write_text(
        """
        -- migrate: no-transaction
        SELECT pg_sleep(0.2);
        CREATE TABLE non_transactional_stop_probe (
            id INTEGER PRIMARY KEY
        );
        """,
        encoding="utf-8",
    )
    (migration_dir / second_version).write_text(
        "CREATE TABLE non_transactional_stop_tail (id INTEGER PRIMARY KEY);\n",
        encoding="utf-8",
    )
    stop_event = asyncio.Event()
    stop_handle = asyncio.get_running_loop().call_later(0.1, stop_event.set)
    started_at = monotonic()
    try:
        rerun = await run_migrations(
            migrated_db["pool"],
            migration_dir,
            stop_requested=stop_event.is_set,
        )
    finally:
        stop_handle.cancel()
    elapsed = monotonic() - started_at

    async with migrated_db["pool"].acquire() as connection:
        status = await connection.fetchval(
            """
            SELECT status
            FROM schema_migrations
            WHERE version = $1
            """,
            first_version,
        )
        relation = await connection.fetchval(
            "SELECT to_regclass('non_transactional_stop_probe')",
        )
        tail_relation = await connection.fetchval(
            "SELECT to_regclass('non_transactional_stop_tail')",
        )

    assert rerun == [first_version]
    assert elapsed >= 0.2
    assert status == "applied"
    assert relation == "non_transactional_stop_probe"
    assert tail_relation is None


@pytest.mark.asyncio
async def test_non_transactional_migration_preserves_sql_errors_during_shutdown(
    migrated_db: dict[str, object],
    tmp_path: Path,
) -> None:
    migration_dir = tmp_path / "migrations"
    migration_dir.mkdir()
    version = "9013_non_transactional_stop_error.sql"
    (migration_dir / version).write_text(
        """
        -- migrate: no-transaction
        SELECT pg_sleep(0.2);
        SELECT 1 / 0;
        """,
        encoding="utf-8",
    )
    stop_event = asyncio.Event()
    stop_handle = asyncio.get_running_loop().call_later(0.1, stop_event.set)
    try:
        with pytest.raises(asyncpg.DivisionByZeroError):
            await run_migrations(
                migrated_db["pool"],
                migration_dir,
                stop_requested=stop_event.is_set,
            )
    finally:
        stop_handle.cancel()

    async with migrated_db["pool"].acquire() as connection:
        status = await connection.fetchval(
            """
            SELECT status
            FROM schema_migrations
            WHERE version = $1
            """,
            version,
        )

    assert status == "failed"
