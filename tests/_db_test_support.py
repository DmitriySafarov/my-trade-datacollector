from __future__ import annotations

import asyncio
import os
from contextlib import suppress
from pathlib import Path

import asyncpg

from src.config.bootstrap import BootstrapConfig
from tests._db_connection import create_pool_with_retry, wait_for_database

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TEST_DB_HOST = "127.0.0.1"
DEFAULT_TEST_DB_PORT = 55432
TEST_DB_SOCKET_DIR = Path(
    os.getenv("TEST_DB_SOCKET_DIR", str(ROOT / ".docker" / "postgres-socket")),
)
TEST_DB_SOCKET_PORT = 5432
TEST_DB_BOOTSTRAP = os.getenv("TEST_DB_BOOTSTRAP", "compose").lower()
TEST_DB_HOST_OVERRIDE = os.getenv("TEST_DB_HOST")
TEST_DB_PORT_OVERRIDE = os.getenv("TEST_DB_PORT")
TEST_DB_USER = "collector"
TEST_DB_PASSWORD = "collector"
TEST_DB_NAME = "collector"
COMPOSE_TEST_DB_PORT = (
    int(TEST_DB_PORT_OVERRIDE)
    if TEST_DB_PORT_OVERRIDE is not None
    else DEFAULT_TEST_DB_PORT
)


def _is_socket_host(host: str | None) -> bool:
    return bool(host and host.startswith("/"))


def _explicit_test_target() -> tuple[str, int]:
    host = TEST_DB_HOST_OVERRIDE or DEFAULT_TEST_DB_HOST
    default_port = (
        TEST_DB_SOCKET_PORT if _is_socket_host(host) else DEFAULT_TEST_DB_PORT
    )
    port = (
        int(TEST_DB_PORT_OVERRIDE)
        if TEST_DB_PORT_OVERRIDE is not None
        else default_port
    )
    return host, port


def _compose_socket_target() -> tuple[str, int]:
    return str(TEST_DB_SOCKET_DIR), TEST_DB_SOCKET_PORT


def _compose_tcp_target() -> tuple[str, int]:
    return DEFAULT_TEST_DB_HOST, COMPOSE_TEST_DB_PORT


if TEST_DB_BOOTSTRAP == "external" or TEST_DB_HOST_OVERRIDE is not None:
    TEST_DB_HOST, TEST_DB_PORT = _explicit_test_target()
else:
    TEST_DB_HOST, TEST_DB_PORT = _compose_tcp_target()

DOCKER_ENV = {
    "DB_PORT": str(COMPOSE_TEST_DB_PORT),
    "POSTGRES_SOCKET_DIR": str(TEST_DB_SOCKET_DIR),
    "DB_USER": TEST_DB_USER,
    "DB_PASSWORD": TEST_DB_PASSWORD,
    "DB_NAME": TEST_DB_NAME,
}


def _test_config(database: str) -> BootstrapConfig:
    return _build_test_config(database, TEST_DB_HOST, TEST_DB_PORT)


def _test_configs(database: str) -> list[BootstrapConfig]:
    if TEST_DB_BOOTSTRAP == "external" or TEST_DB_HOST_OVERRIDE is not None:
        return [_test_config(database)]
    if TEST_DB_BOOTSTRAP != "compose":
        raise RuntimeError(
            "TEST_DB_BOOTSTRAP must be either 'compose' or 'external'.",
        )
    return [
        _build_test_config(database, *_compose_socket_target()),
        _build_test_config(database, *_compose_tcp_target()),
    ]


def _build_test_config(
    database: str,
    db_host: str,
    db_port: int,
) -> BootstrapConfig:
    return BootstrapConfig(
        db_host=db_host,
        db_port=db_port,
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


async def _wait_for_database(
    database: str,
    *,
    timeout: float = 60.0,
    allow_missing_database: bool = False,
) -> None:
    await wait_for_database(
        _test_configs(database),
        timeout=timeout,
        allow_missing_database=allow_missing_database,
    )


async def _create_pool_with_retry(
    database: str,
    *,
    timeout: float = 60.0,
    allow_missing_database: bool = False,
) -> asyncpg.Pool:
    return await create_pool_with_retry(
        _test_configs(database),
        timeout=timeout,
        allow_missing_database=allow_missing_database,
    )


async def _ensure_postgres_service() -> None:
    if TEST_DB_BOOTSTRAP == "external":
        await _wait_for_database("postgres")
        return
    if TEST_DB_BOOTSTRAP != "compose":
        raise RuntimeError(
            "TEST_DB_BOOTSTRAP must be either 'compose' or 'external'.",
        )

    TEST_DB_SOCKET_DIR.mkdir(parents=True, exist_ok=True)
    TEST_DB_SOCKET_DIR.chmod(0o777)

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
