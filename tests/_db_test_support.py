from __future__ import annotations

import asyncio
import os
from contextlib import suppress
from errno import EPERM
from pathlib import Path

import asyncpg

from src.config.bootstrap import BootstrapConfig
from src.db.pool import create_pool

ROOT = Path(__file__).resolve().parents[1]
TEST_DB_HOST = "127.0.0.1"
TEST_DB_PORT = 55432
TEST_DB_USER = "collector"
TEST_DB_PASSWORD = "collector"
TEST_DB_NAME = "collector"
DOCKER_ENV = {
    "DB_PORT": str(TEST_DB_PORT),
    "DB_USER": TEST_DB_USER,
    "DB_PASSWORD": TEST_DB_PASSWORD,
    "DB_NAME": TEST_DB_NAME,
}
TRANSIENT_CONNECTION_ERRORS = (
    asyncpg.CannotConnectNowError,
    asyncpg.ConnectionFailureError,
    asyncpg.ConnectionDoesNotExistError,
    asyncpg.TooManyConnectionsError,
)
AUTH_CONFIGURATION_ERRORS = (
    asyncpg.InvalidAuthorizationSpecificationError,
    asyncpg.InvalidPasswordError,
)


def _test_config(database: str) -> BootstrapConfig:
    return BootstrapConfig(
        db_host=TEST_DB_HOST,
        db_port=TEST_DB_PORT,
        db_name=database,
        db_user=TEST_DB_USER,
        db_password=TEST_DB_PASSWORD,
        db_ssl="disable",
        service_name="pytest",
        log_level="INFO",
        health_host="127.0.0.1",
        health_port=0,
        db_pool_min_size=1,
        db_pool_max_size=1,
        settings_refresh_seconds=30,
        default_batch_count=500,
        default_batch_seconds=2.0,
        collector_startup_timeout_seconds=15.0,
        shutdown_timeout_seconds=15,
        finnhub_api_key=None,
        fred_api_key=None,
        binance_rest_base_url="https://fapi.binance.com",
        hyperliquid_api_url="https://api.hyperliquid.xyz",
        allow_non_transactional_migrations=True,
    )


def _is_blocked_socket_error(error: BaseException) -> bool:
    current: BaseException | None = error
    while current is not None:
        if isinstance(current, OSError) and current.errno == EPERM:
            return True
        current = current.__cause__
    return False


def _is_retryable_connect_error(
    error: BaseException,
    *,
    allow_missing_database: bool = False,
) -> bool:
    if _is_blocked_socket_error(error) or isinstance(error, AUTH_CONFIGURATION_ERRORS):
        return False
    if allow_missing_database and isinstance(error, asyncpg.InvalidCatalogNameError):
        return True
    if isinstance(error, OSError):
        return True
    return isinstance(error, TRANSIENT_CONNECTION_ERRORS)


def _raise_for_non_retryable_config(error: BaseException) -> None:
    if _is_blocked_socket_error(error):
        raise RuntimeError(
            "Sandbox blocked local PostgreSQL socket access during DB tests.",
        ) from error
    if isinstance(error, AUTH_CONFIGURATION_ERRORS):
        raise RuntimeError(
            "Pinned Docker Postgres credentials do not match the running "
            "postgres-data volume. Reinitialize the compose volume or align "
            "the credentials before running DB tests.",
        ) from error


async def _connect_once(database: str) -> None:
    config = _test_config(database)
    connection = await asyncpg.connect(
        host=config.db_host,
        port=config.db_port,
        user=config.db_user,
        password=config.db_password,
        database=config.db_name,
        ssl=None,
        server_settings={"TimeZone": "UTC"},
    )
    await connection.close()


async def _wait_for_database(
    database: str,
    *,
    timeout: float = 60.0,
    allow_missing_database: bool = False,
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        try:
            await _connect_once(database)
        except Exception as error:
            _raise_for_non_retryable_config(error)
            if (
                not _is_retryable_connect_error(
                    error,
                    allow_missing_database=allow_missing_database,
                )
                or asyncio.get_running_loop().time() >= deadline
            ):
                raise
            await asyncio.sleep(1)
            continue
        return


async def _create_pool_with_retry(
    database: str,
    *,
    timeout: float = 60.0,
    allow_missing_database: bool = False,
) -> asyncpg.Pool:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        try:
            await _wait_for_database(
                database,
                timeout=timeout,
                allow_missing_database=allow_missing_database,
            )
            return await create_pool(_test_config(database))
        except Exception as error:
            _raise_for_non_retryable_config(error)
            if (
                not _is_retryable_connect_error(
                    error,
                    allow_missing_database=allow_missing_database,
                )
                or asyncio.get_running_loop().time() >= deadline
            ):
                raise
            await asyncio.sleep(1)


async def _ensure_postgres_service() -> None:
    env = os.environ.copy()
    env.update(DOCKER_ENV)
    try:
        process = await asyncio.create_subprocess_exec(
            "docker",
            "compose",
            "up",
            "-d",
            "postgres",
            cwd=str(ROOT),
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError as error:
        raise RuntimeError(
            "docker compose is required for DB tests but is not installed.",
        ) from error

    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=60)
    except asyncio.TimeoutError as error:
        with suppress(ProcessLookupError):
            process.kill()
        with suppress(ProcessLookupError):
            await process.wait()
        raise RuntimeError("docker compose up -d postgres timed out.") from error
    if process.returncode != 0:
        output = (stderr or stdout).decode().strip()
        raise RuntimeError(f"docker compose up -d postgres failed: {output}")

    await _wait_for_database("postgres")
