from __future__ import annotations

import asyncio
from pathlib import Path
from time import monotonic

import pytest

from src.db.migrations import run_migrations


@pytest.mark.asyncio
async def test_run_migrations_interrupts_blocked_statement_when_shutdown_requested(
    migrated_db: dict[str, object],
    tmp_path: Path,
) -> None:
    migration_dir = tmp_path / "migrations"
    migration_dir.mkdir()
    version = "9011_blocked_statement.sql"
    (migration_dir / version).write_text(
        """
        SELECT pg_sleep(5);
        CREATE TABLE blocked_statement_probe (id INTEGER PRIMARY KEY);
        """,
        encoding="utf-8",
    )

    stop_event = asyncio.Event()
    stop_handle = asyncio.get_running_loop().call_later(0.05, stop_event.set)
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
        relation = await connection.fetchval(
            "SELECT to_regclass('blocked_statement_probe')",
        )
        applied_count = await connection.fetchval(
            """
            SELECT count(*)
            FROM schema_migrations
            WHERE version = $1
            """,
            version,
        )

    assert rerun == []
    assert elapsed < 2
    assert relation is None
    assert applied_count == 0


@pytest.mark.asyncio
async def test_run_migrations_cancels_in_flight_statement_on_outer_cancellation(
    migrated_db: dict[str, object],
    tmp_path: Path,
) -> None:
    migration_dir = tmp_path / "migrations"
    migration_dir.mkdir()
    (migration_dir / "9012_cancel_probe.sql").write_text(
        """
        SELECT pg_sleep(5);
        CREATE TABLE cancelled_statement_probe (id INTEGER PRIMARY KEY);
        """,
        encoding="utf-8",
    )
    followup_dir = tmp_path / "followup"
    followup_dir.mkdir()
    followup_version = "9013_followup_probe.sql"
    (followup_dir / followup_version).write_text(
        "CREATE TABLE followup_probe (id INTEGER PRIMARY KEY);\n",
        encoding="utf-8",
    )

    task = asyncio.create_task(run_migrations(migrated_db["pool"], migration_dir))
    await asyncio.sleep(0.05)
    started_at = monotonic()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    elapsed = monotonic() - started_at
    rerun = await run_migrations(migrated_db["pool"], followup_dir)

    async with migrated_db["pool"].acquire() as connection:
        cancelled_probe = await connection.fetchval(
            "SELECT to_regclass('cancelled_statement_probe')",
        )
        followup_probe = await connection.fetchval(
            "SELECT to_regclass('followup_probe')",
        )

    assert elapsed < 1
    assert rerun == [followup_version]
    assert cancelled_probe is None
    assert followup_probe == "followup_probe"
