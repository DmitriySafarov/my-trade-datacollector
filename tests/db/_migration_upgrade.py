from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncIterator

import asyncpg

from src.db.migrations import run_migrations
from src.db.pool import close_pool
from tests._db_test_support import (
    ROOT,
    _create_pool_with_retry,
    _ensure_postgres_service,
)


def utc(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def write_migration_subset(tmp_path: Path, name: str, filenames: set[str]) -> Path:
    migration_dir = tmp_path / name
    migration_dir.mkdir()
    for source in sorted((ROOT / "migrations").glob("*.sql")):
        if source.name in filenames:
            (migration_dir / source.name).write_text(
                source.read_text(encoding="utf-8"),
                encoding="utf-8",
            )
    return migration_dir


@asynccontextmanager
async def upgrade_db(tmp_path: Path) -> AsyncIterator[asyncpg.Pool]:
    await _ensure_postgres_service()
    db_name = f"collector_upgrade_{uuid.uuid4().hex[:12]}"
    admin_pool = await _create_pool_with_retry("postgres")
    pool = None
    try:
        async with admin_pool.acquire() as connection:
            await connection.execute(f'CREATE DATABASE "{db_name}"')
        pool = await _create_pool_with_retry(db_name, allow_missing_database=True)
        initial = write_migration_subset(
            tmp_path,
            f"{db_name}_initial",
            {
                path.name
                for path in (ROOT / "migrations").glob("*.sql")
                if path.name <= "0011_runtime_default_repairs.sql"
            },
        )
        await run_migrations(pool, initial)
        yield pool
    finally:
        if pool is not None:
            await close_pool(pool)
        async with admin_pool.acquire() as connection:
            await connection.execute(
                f'DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)'
            )
        await close_pool(admin_pool)


async def apply_repair_migrations(
    pool: asyncpg.Pool,
    tmp_path: Path,
    name: str,
) -> list[str]:
    repair_dir = write_migration_subset(
        tmp_path,
        name,
        {
            path.name
            for path in (ROOT / "migrations").glob("*.sql")
            if path.name >= "0012_bronze_dedup_repairs.sql"
        },
    )
    return await run_migrations(pool, repair_dir)


async def compress_tables(pool: asyncpg.Pool, *tables: str) -> None:
    async with pool.acquire() as connection:
        for table in tables:
            await connection.fetch(
                """
                SELECT compress_chunk(chunk_info.chunk, if_not_compressed => TRUE)
                FROM show_chunks($1) AS chunk_info(chunk)
                """,
                table,
            )
