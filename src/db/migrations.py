from __future__ import annotations

import asyncio
from collections.abc import Callable
from pathlib import Path

import asyncpg

from src.db.migration_lock import (
    MIGRATION_LOCK_ID,
    MIGRATION_LOCK_RETRY_SECONDS,
    acquire_migration_lock,
)
from src.db.migration_plan import PlannedMigration, build_migration_plan
from src.db.migration_state import (
    ensure_migration_metadata,
    mark_migration_applied,
    mark_migration_failed,
    mark_migration_running,
)
from src.db.migration_runtime import connect_for_migrations, execute_statement
from src.db.migration_sql import split_sql_statements
from src.db.pool import pool_config


MIGRATION_SESSION_SQL = "SET statement_timeout = 0"


class MigrationInterruptedError(RuntimeError):
    pass


async def run_migrations(
    pool: asyncpg.Pool,
    migrations_dir: Path,
    *,
    stop_requested: Callable[[], bool] | None = None,
    lock_retry_seconds: float = MIGRATION_LOCK_RETRY_SECONDS,
) -> list[str]:
    migration_files = sorted(
        path for path in migrations_dir.glob("*.sql") if path.is_file()
    )
    applied: list[str] = []
    lock_acquired = False
    connection = await connect_for_migrations(pool)
    try:
        try:
            lock_acquired = await acquire_migration_lock(
                connection,
                stop_requested=stop_requested,
                lock_retry_seconds=lock_retry_seconds,
            )
            if not lock_acquired:
                return []
            if stop_requested is not None and stop_requested():
                return []
            await connection.execute(MIGRATION_SESSION_SQL, timeout=None)
            await ensure_migration_metadata(connection)
            config = pool_config(pool)
            pending = await build_migration_plan(
                connection,
                migration_files,
                allow_non_transactional=bool(
                    config and config.allow_non_transactional_migrations
                ),
            )
            for migration in pending:
                if stop_requested is not None and stop_requested():
                    return applied
                try:
                    await _run_migration_file(
                        connection,
                        migration=migration,
                        stop_requested=stop_requested,
                    )
                except MigrationInterruptedError:
                    return applied
                applied.append(migration.version)
        finally:
            if lock_acquired and not connection.is_closed():
                await connection.execute(
                    "SELECT pg_advisory_unlock($1)",
                    MIGRATION_LOCK_ID,
                    timeout=None,
                )
    finally:
        if not connection.is_closed():
            await connection.close()
    return applied


async def _run_migration_file(
    connection: asyncpg.Connection,
    *,
    migration: PlannedMigration,
    stop_requested: Callable[[], bool] | None,
) -> None:
    statements = split_sql_statements(migration.migration_sql)
    if migration.non_transactional:
        await _run_non_transactional_migration(
            connection,
            statements=statements,
            version=migration.version,
            content_hash=migration.content_hash,
        )
        return
    try:
        async with connection.transaction():
            await _execute_migration_statements(
                connection,
                statements=statements,
                stop_requested=stop_requested,
                version=migration.version,
            )
            await mark_migration_applied(
                connection,
                migration.version,
                migration.content_hash,
            )
    except asyncio.CancelledError as error:
        if stop_requested is not None and stop_requested():
            raise MigrationInterruptedError(migration.version) from error
        raise
    except asyncpg.InterfaceError as error:
        if not connection.is_closed():
            raise
        if stop_requested is not None and stop_requested():
            raise MigrationInterruptedError(migration.version) from error
        task = asyncio.current_task()
        if task is not None and task.cancelling():
            raise asyncio.CancelledError() from error
        raise


async def _run_non_transactional_migration(
    connection: asyncpg.Connection,
    *,
    statements: list[str],
    version: str,
    content_hash: str,
) -> None:
    await mark_migration_running(connection, version, content_hash)
    execution = asyncio.create_task(
        _execute_migration_statements(
            connection,
            statements=statements,
            stop_requested=None,
            version=version,
        )
    )
    try:
        await asyncio.shield(execution)
    except asyncio.CancelledError as error:
        try:
            await execution
        except Exception:
            await mark_migration_failed(connection, version)
            raise
        await mark_migration_applied(connection, version, content_hash)
        raise error
    except Exception:
        await mark_migration_failed(connection, version)
        raise
    await mark_migration_applied(connection, version, content_hash)


async def _execute_migration_statements(
    connection: asyncpg.Connection,
    *,
    statements: list[str],
    stop_requested: Callable[[], bool] | None,
    version: str,
) -> None:
    for statement in statements:
        if stop_requested is not None and stop_requested():
            raise MigrationInterruptedError(version)
        await execute_statement(
            connection,
            statement=statement,
            stop_requested=stop_requested,
            interrupted_error=MigrationInterruptedError,
            version=version,
        )
