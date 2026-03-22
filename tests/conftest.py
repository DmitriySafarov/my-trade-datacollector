from __future__ import annotations

import uuid

import asyncpg
import pytest_asyncio

from tests._db_test_support import (
    ROOT,
    _create_pool_with_retry,
    _ensure_postgres_service,
    _test_config,
)
from src.db.migrations import run_migrations
from src.db.pool import close_pool


@pytest_asyncio.fixture
async def migrated_db() -> dict[str, object]:
    await _ensure_postgres_service()

    admin_pool: asyncpg.Pool | None = None
    pool: asyncpg.Pool | None = None
    db_created = False
    cleanup_may_raise = True
    cleanup_error: Exception | None = None
    db_name = f"collector_test_{uuid.uuid4().hex[:12]}"

    try:
        admin_pool = await _create_pool_with_retry("postgres")
        async with admin_pool.acquire() as connection:
            await connection.execute(f'CREATE DATABASE "{db_name}"')
        db_created = True

        pool = await _create_pool_with_retry(db_name, allow_missing_database=True)
        applied = await run_migrations(pool, ROOT / "migrations")

        yield {"pool": pool, "applied": applied, "config": _test_config(db_name)}
        cleanup_may_raise = False
    finally:
        if pool is not None:
            try:
                await close_pool(pool)
            except Exception as error:
                cleanup_error = cleanup_error or error

        if admin_pool is not None:
            if db_created:
                try:
                    async with admin_pool.acquire() as connection:
                        await connection.execute(
                            f'DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)'
                        )
                except Exception as error:
                    cleanup_error = cleanup_error or error
            try:
                await close_pool(admin_pool)
            except Exception as error:
                cleanup_error = cleanup_error or error

        if cleanup_error is not None and not cleanup_may_raise:
            raise cleanup_error
