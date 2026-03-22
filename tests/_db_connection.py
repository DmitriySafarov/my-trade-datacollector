from __future__ import annotations

import asyncio
from collections.abc import Sequence
from errno import EPERM

import asyncpg

from src.config.bootstrap import BootstrapConfig
from src.db.pool import create_pool

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


def raise_for_non_retryable_config(error: BaseException) -> None:
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


async def wait_for_database(
    config: BootstrapConfig | Sequence[BootstrapConfig],
    *,
    timeout: float = 60.0,
    allow_missing_database: bool = False,
) -> BootstrapConfig:
    configs = _config_candidates(config)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        retryable_error: BaseException | None = None
        candidate_terminal_error: BaseException | None = None
        global_terminal_error: BaseException | None = None
        for candidate in configs:
            try:
                await _connect_once(candidate)
            except Exception as error:
                status = _connect_error_status(
                    candidate,
                    error,
                    allow_missing_database=allow_missing_database,
                )
                if status == "retryable":
                    retryable_error = error
                elif status == "candidate_terminal":
                    candidate_terminal_error = candidate_terminal_error or error
                else:
                    global_terminal_error = global_terminal_error or error
                continue
            return candidate
        if retryable_error is None:
            terminal_error = global_terminal_error or candidate_terminal_error
            if terminal_error is not None:
                raise_for_non_retryable_config(terminal_error)
                raise terminal_error
        if loop.time() >= deadline:
            if retryable_error is not None:
                raise retryable_error
            terminal_error = global_terminal_error or candidate_terminal_error
            if terminal_error is not None:
                raise_for_non_retryable_config(terminal_error)
                raise terminal_error
            raise TimeoutError("database connection timed out without an error")
        await asyncio.sleep(1)


async def create_pool_with_retry(
    config: BootstrapConfig | Sequence[BootstrapConfig],
    *,
    timeout: float = 60.0,
    allow_missing_database: bool = False,
) -> asyncpg.Pool:
    pool, _ = await create_pool_with_selected_config(
        config,
        timeout=timeout,
        allow_missing_database=allow_missing_database,
    )
    return pool


async def create_pool_with_selected_config(
    config: BootstrapConfig | Sequence[BootstrapConfig],
    *,
    timeout: float = 60.0,
    allow_missing_database: bool = False,
) -> tuple[asyncpg.Pool, BootstrapConfig]:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        try:
            selected = await wait_for_database(
                config,
                timeout=max(deadline - asyncio.get_running_loop().time(), 1),
                allow_missing_database=allow_missing_database,
            )
            return await create_pool(selected), selected
        except Exception as error:
            raise_for_non_retryable_config(error)
            if not _should_retry(
                error,
                deadline=deadline,
                allow_missing_database=allow_missing_database,
            ):
                raise
            await asyncio.sleep(1)


def _config_candidates(
    config: BootstrapConfig | Sequence[BootstrapConfig],
) -> list[BootstrapConfig]:
    if isinstance(config, BootstrapConfig):
        return [config]
    return list(config)


def _is_blocked_socket_error(error: BaseException) -> bool:
    current: BaseException | None = error
    while current is not None:
        if isinstance(current, OSError) and current.errno == EPERM:
            return True
        current = current.__cause__
    return False


def _connect_error_status(
    config: BootstrapConfig,
    error: BaseException,
    *,
    allow_missing_database: bool = False,
) -> str:
    if isinstance(error, AUTH_CONFIGURATION_ERRORS):
        return "global_terminal"
    if _is_blocked_socket_error(error):
        return (
            "candidate_terminal"
            if config.db_host.startswith("/")
            else "global_terminal"
        )
    if allow_missing_database and isinstance(error, asyncpg.InvalidCatalogNameError):
        return "retryable"
    if isinstance(error, OSError) or isinstance(error, TRANSIENT_CONNECTION_ERRORS):
        return "retryable"
    return "global_terminal"


def _is_retryable_connect_error(
    error: BaseException,
    *,
    allow_missing_database: bool = False,
) -> bool:
    if _is_blocked_socket_error(error) or isinstance(error, AUTH_CONFIGURATION_ERRORS):
        return False
    if allow_missing_database and isinstance(error, asyncpg.InvalidCatalogNameError):
        return True
    return isinstance(error, OSError) or isinstance(error, TRANSIENT_CONNECTION_ERRORS)


def _should_retry(
    error: BaseException,
    *,
    deadline: float,
    allow_missing_database: bool = False,
) -> bool:
    return (
        _is_retryable_connect_error(
            error,
            allow_missing_database=allow_missing_database,
        )
        and asyncio.get_running_loop().time() < deadline
    )


async def _connect_once(config: BootstrapConfig) -> None:
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
