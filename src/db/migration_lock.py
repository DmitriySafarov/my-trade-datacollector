from __future__ import annotations

import asyncio
from collections.abc import Callable

import asyncpg


MIGRATION_LOCK_ID = 4_296_041_632_503
MIGRATION_LOCK_RETRY_SECONDS = 0.5


async def acquire_migration_lock(
    connection: asyncpg.Connection,
    *,
    stop_requested: Callable[[], bool] | None,
    lock_retry_seconds: float,
) -> bool:
    while True:
        if stop_requested is not None and stop_requested():
            return False

        acquired = await connection.fetchval(
            "SELECT pg_try_advisory_lock($1)",
            MIGRATION_LOCK_ID,
            timeout=None,
        )
        if acquired:
            return True

        await asyncio.sleep(lock_retry_seconds)
