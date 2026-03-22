from __future__ import annotations

import asyncio
from collections.abc import Callable

import asyncpg

from src.db.pool import connection_kwargs, pool_config


MIGRATION_STATEMENT_POLL_SECONDS = 0.1


async def connect_for_migrations(pool: asyncpg.Pool) -> asyncpg.Connection:
    config = pool_config(pool)
    if config is None:
        raise RuntimeError(
            "Migration connection requires a bootstrap config on the pool."
        )
    return await asyncpg.connect(**connection_kwargs(config, command_timeout=None))


async def cancel_execution(
    connection: asyncpg.Connection,
    execution: asyncio.Task[str],
) -> None:
    execution.cancel()
    if not connection.is_closed():
        connection.terminate()
    try:
        await execution
    except asyncio.CancelledError:
        pass
    except Exception:
        pass


async def execute_statement(
    connection: asyncpg.Connection,
    *,
    statement: str,
    stop_requested: Callable[[], bool] | None,
    interrupted_error: type[RuntimeError],
    version: str,
) -> None:
    execution = asyncio.create_task(connection.execute(statement, timeout=None))
    try:
        while True:
            done, _ = await asyncio.wait(
                {execution},
                timeout=MIGRATION_STATEMENT_POLL_SECONDS,
            )
            if execution in done:
                await execution
                return
            if stop_requested is not None and stop_requested():
                await cancel_execution(connection, execution)
                raise interrupted_error(version)
    except asyncio.CancelledError as error:
        await cancel_execution(connection, execution)
        if stop_requested is not None and stop_requested():
            raise interrupted_error(version) from error
        raise
