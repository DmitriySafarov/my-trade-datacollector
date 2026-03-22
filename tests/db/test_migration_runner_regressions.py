from __future__ import annotations

import asyncio
from pathlib import Path

import asyncpg
import pytest

from src.db.migration_sql import split_sql_statements
from src.db.migrations import run_migrations
from src.db.pool import close_pool, pool_config, register_pool_config


def test_split_sql_statements_preserves_dollar_quoted_functions() -> None:
    statements = split_sql_statements(
        """
        CREATE OR REPLACE FUNCTION parser_probe() RETURNS void AS $body$
        BEGIN
            PERFORM 1;
            PERFORM 2;
        END;
        $body$ LANGUAGE plpgsql;

        CREATE TABLE parser_probe_table (
            id INTEGER PRIMARY KEY
        );
        """,
    )

    assert len(statements) == 2
    assert "PERFORM 1;" in statements[0]
    assert statements[1].startswith("CREATE TABLE parser_probe_table")


@pytest.mark.asyncio
async def test_run_migrations_rolls_back_current_file_when_shutdown_requested(
    migrated_db: dict[str, object],
    tmp_path: Path,
) -> None:
    migration_dir = tmp_path / "migrations"
    migration_dir.mkdir()
    versions = ["9003_apply_then_stop.sql", "9004_should_not_run.sql"]
    (migration_dir / versions[0]).write_text(
        """
        CREATE TABLE IF NOT EXISTS post_lock_stop_probe (
            id INTEGER PRIMARY KEY
        );
        SELECT pg_sleep(0.1);
        INSERT INTO post_lock_stop_probe (id) VALUES (1);
        """,
        encoding="utf-8",
    )
    (migration_dir / versions[1]).write_text(
        "CREATE TABLE should_not_exist (id INTEGER PRIMARY KEY);\n",
        encoding="utf-8",
    )

    stop_event = asyncio.Event()
    stop_handle = asyncio.get_running_loop().call_later(0.02, stop_event.set)
    try:
        rerun = await run_migrations(
            migrated_db["pool"],
            migration_dir,
            stop_requested=stop_event.is_set,
        )
    finally:
        stop_handle.cancel()

    async with migrated_db["pool"].acquire() as connection:
        applied_versions = {
            row["version"]
            for row in await connection.fetch(
                """
                SELECT version
                FROM schema_migrations
                WHERE version = ANY($1::text[])
                """,
                versions,
            )
        }
        should_not_exist = await connection.fetchval(
            "SELECT to_regclass('should_not_exist')"
        )
        post_lock_stop_probe = await connection.fetchval(
            "SELECT to_regclass('post_lock_stop_probe')"
        )

    assert rerun == []
    assert applied_versions == set()
    assert should_not_exist is None
    assert post_lock_stop_probe is None


@pytest.mark.asyncio
async def test_run_migrations_reads_only_pending_files(
    migrated_db: dict[str, object],
    tmp_path: Path,
) -> None:
    migration_dir = tmp_path / "migrations"
    migration_dir.mkdir()
    (migration_dir / "9005_applied.sql").mkdir()
    pending_version = "9006_pending.sql"
    (migration_dir / pending_version).write_text(
        """
        CREATE TABLE IF NOT EXISTS pending_only_probe (
            id INTEGER PRIMARY KEY
        );
        """,
        encoding="utf-8",
    )

    rerun = await run_migrations(migrated_db["pool"], migration_dir)

    async with migrated_db["pool"].acquire() as connection:
        pending_only_probe = await connection.fetchval(
            "SELECT to_regclass('pending_only_probe')",
        )

    assert rerun == [pending_version]
    assert pending_only_probe == "pending_only_probe"


@pytest.mark.asyncio
async def test_run_migrations_ignores_unapplied_sql_directories(
    migrated_db: dict[str, object],
    tmp_path: Path,
) -> None:
    migration_dir = tmp_path / "migrations"
    migration_dir.mkdir()
    (migration_dir / "9007_directory.sql").mkdir()
    version = "9008_real.sql"
    (migration_dir / version).write_text(
        "CREATE TABLE ignored_directory_probe (id INTEGER PRIMARY KEY);\n",
        encoding="utf-8",
    )

    rerun = await run_migrations(migrated_db["pool"], migration_dir)

    async with migrated_db["pool"].acquire() as connection:
        relation = await connection.fetchval(
            "SELECT to_regclass('ignored_directory_probe')",
        )

    assert rerun == [version]
    assert relation == "ignored_directory_probe"


@pytest.mark.asyncio
async def test_run_migrations_disable_statement_timeout_per_command(
    migrated_db: dict[str, object],
    tmp_path: Path,
) -> None:
    config = migrated_db["config"]
    migration_dir = tmp_path / "migrations"
    migration_dir.mkdir()
    version = "9009_timeout_probe.sql"
    (migration_dir / version).write_text(
        """
        SELECT pg_sleep(0.05);
        CREATE TABLE timeout_probe (id INTEGER PRIMARY KEY);
        """,
        encoding="utf-8",
    )

    pool = await asyncpg.create_pool(
        host=config.db_host,
        port=config.db_port,
        database=config.db_name,
        user=config.db_user,
        password=config.db_password,
        min_size=1,
        max_size=1,
        command_timeout=0.01,
        ssl=None if config.db_ssl == "disable" else config.db_ssl,
        server_settings={"TimeZone": "UTC"},
    )
    register_pool_config(pool, config)
    try:
        rerun = await run_migrations(pool, migration_dir)
    finally:
        await close_pool(pool)

    async with migrated_db["pool"].acquire() as connection:
        relation = await connection.fetchval("SELECT to_regclass('timeout_probe')")

    assert rerun == [version]
    assert relation == "timeout_probe"
    assert pool_config(pool) is None
