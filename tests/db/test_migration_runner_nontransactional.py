from __future__ import annotations

import uuid
from dataclasses import replace
from pathlib import Path

import asyncpg
import pytest

from src.db.migration_manifest import (
    COMPATIBLE_MIGRATION_HASHES,
    STARTUP_NON_TRANSACTIONAL_MIGRATIONS,
    TRUSTED_MIGRATION_HASHES,
)
from src.db.migration_plan import (
    MigrationHashMismatchError,
    MissingMigrationHashError,
    NonTransactionalMigrationMaintenanceRequiredError,
)
from src.db.migrations import run_migrations
from src.db.migration_state import IncompleteNonTransactionalMigrationError
from src.db.pool import close_pool, create_pool
from tests._db_test_support import (
    ROOT,
    _create_pool_with_retry,
    _ensure_postgres_service,
    _test_config,
    _wait_for_database,
)


@pytest.mark.asyncio
async def test_run_migrations_applies_committed_non_transactional_chain_on_normal_boot() -> (
    None
):
    await _ensure_postgres_service()
    admin_pool: asyncpg.Pool | None = None
    pool: asyncpg.Pool | None = None
    db_created = False
    db_name = f"collector_test_{uuid.uuid4().hex[:12]}"
    config = replace(_test_config(db_name), allow_non_transactional_migrations=False)
    try:
        admin_pool = await _create_pool_with_retry("postgres")
        async with admin_pool.acquire() as connection:
            await connection.execute(f'CREATE DATABASE "{db_name}"')
        db_created = True
        await _wait_for_database(db_name, allow_missing_database=True)
        pool = await create_pool(config)
        applied = await run_migrations(pool, ROOT / "migrations")
    finally:
        if pool is not None:
            await close_pool(pool)
        if admin_pool is not None:
            if db_created:
                async with admin_pool.acquire() as connection:
                    await connection.execute(
                        f'DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)'
                    )
            await close_pool(admin_pool)

    assert STARTUP_NON_TRANSACTIONAL_MIGRATIONS.issubset(set(applied))


@pytest.mark.asyncio
async def test_run_migrations_requires_maintenance_mode_to_resume_non_transactional_files(
    migrated_db: dict[str, object],
    tmp_path: Path,
) -> None:
    migration_dir = tmp_path / "migrations"
    migration_dir.mkdir()
    version = "9011_non_transactional_resume_probe.sql"
    (migration_dir / version).write_text(
        """
        -- migrate: no-transaction
        CREATE TABLE non_transactional_resume_probe (id INTEGER PRIMARY KEY);
        SELECT missing_resume_probe();
        """,
        encoding="utf-8",
    )
    config = replace(
        migrated_db["config"],
        allow_non_transactional_migrations=False,
    )
    maintenance_config = replace(
        migrated_db["config"],
        allow_non_transactional_migrations=True,
    )
    maintenance_pool = await create_pool(maintenance_config)
    try:
        with pytest.raises(asyncpg.UndefinedFunctionError):
            await run_migrations(maintenance_pool, migration_dir)
    finally:
        await close_pool(maintenance_pool)

    pool = await create_pool(config)
    try:
        with pytest.raises(IncompleteNonTransactionalMigrationError):
            await run_migrations(pool, migration_dir)
    finally:
        await close_pool(pool)


@pytest.mark.asyncio
async def test_run_migrations_requires_maintenance_mode_for_unlisted_non_transactional_files(
    migrated_db: dict[str, object],
    tmp_path: Path,
) -> None:
    migration_dir = tmp_path / "migrations"
    migration_dir.mkdir()
    version = "9012_non_transactional_probe.sql"
    (migration_dir / version).write_text(
        """
        -- migrate: no-transaction
        CREATE TABLE non_transactional_probe (id INTEGER PRIMARY KEY);
        SELECT missing_migration_probe();
        """,
        encoding="utf-8",
    )
    config = replace(
        migrated_db["config"],
        allow_non_transactional_migrations=False,
    )
    pool = await create_pool(config)
    try:
        with pytest.raises(NonTransactionalMigrationMaintenanceRequiredError):
            await run_migrations(pool, migration_dir)
    finally:
        await close_pool(pool)

    async with migrated_db["pool"].acquire() as connection:
        relation = await connection.fetchval(
            "SELECT to_regclass('non_transactional_probe')",
        )

    assert relation is None


@pytest.mark.asyncio
async def test_run_migrations_detects_applied_file_hash_drift(
    migrated_db: dict[str, object],
    tmp_path: Path,
) -> None:
    migration_dir = tmp_path / "migrations"
    migration_dir.mkdir()
    version = "9013_hash_probe.sql"
    (migration_dir / version).write_text(
        "CREATE TABLE hash_probe (id INTEGER PRIMARY KEY);\n",
        encoding="utf-8",
    )

    rerun = await run_migrations(migrated_db["pool"], migration_dir)
    (migration_dir / version).write_text(
        "CREATE TABLE hash_probe (id INTEGER PRIMARY KEY, note TEXT);\n",
        encoding="utf-8",
    )

    with pytest.raises(MigrationHashMismatchError):
        await run_migrations(migrated_db["pool"], migration_dir)

    async with migrated_db["pool"].acquire() as connection:
        relation = await connection.fetchval(
            "SELECT to_regclass('hash_probe')",
        )

    assert rerun == [version]
    assert relation == "hash_probe"


@pytest.mark.asyncio
async def test_run_migrations_fails_closed_for_applied_rows_missing_hashes(
    migrated_db: dict[str, object],
    tmp_path: Path,
) -> None:
    migration_dir = tmp_path / "migrations"
    migration_dir.mkdir()
    version = "9014_legacy_hash_gap.sql"
    (migration_dir / version).write_text(
        "CREATE TABLE legacy_hash_gap_probe (id INTEGER PRIMARY KEY);\n",
        encoding="utf-8",
    )

    async with migrated_db["pool"].acquire() as connection:
        await connection.execute(
            """
            INSERT INTO schema_migrations (version, status, content_hash)
            VALUES ($1, 'applied', NULL)
            ON CONFLICT (version) DO UPDATE
            SET status = EXCLUDED.status,
                content_hash = EXCLUDED.content_hash
            """,
            version,
        )

    with pytest.raises(MissingMigrationHashError):
        await run_migrations(migrated_db["pool"], migration_dir)


@pytest.mark.asyncio
async def test_run_migrations_backfills_hashes_for_trusted_legacy_rows(
    migrated_db: dict[str, object],
) -> None:
    version = "0002_runtime_tables.sql"

    async with migrated_db["pool"].acquire() as connection:
        await connection.execute(
            """
            UPDATE schema_migrations
            SET content_hash = NULL
            WHERE version = $1
            """,
            version,
        )

    rerun = await run_migrations(migrated_db["pool"], Path("migrations"))

    async with migrated_db["pool"].acquire() as connection:
        restored_hash = await connection.fetchval(
            """
            SELECT content_hash
            FROM schema_migrations
            WHERE version = $1
            """,
            version,
        )

    assert rerun == []
    assert restored_hash == TRUSTED_MIGRATION_HASHES[version]


@pytest.mark.asyncio
async def test_run_migrations_accepts_known_legacy_hash_variants(
    migrated_db: dict[str, object],
) -> None:
    version = "0002_runtime_tables.sql"
    compatible_hashes = COMPATIBLE_MIGRATION_HASHES[version]
    legacy_hash = next(
        value
        for value in compatible_hashes
        if value != TRUSTED_MIGRATION_HASHES[version]
    )

    async with migrated_db["pool"].acquire() as connection:
        await connection.execute(
            """
            UPDATE schema_migrations
            SET content_hash = $2
            WHERE version = $1
            """,
            version,
            legacy_hash,
        )

    rerun = await run_migrations(migrated_db["pool"], Path("migrations"))

    assert rerun == []
