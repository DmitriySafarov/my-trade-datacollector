from __future__ import annotations

import asyncpg

from src.config.bootstrap import BootstrapConfig


_POOL_CONFIGS: dict[int, BootstrapConfig] = {}


def _ssl_value(db_ssl: str) -> str | None:
    return None if db_ssl == "disable" else db_ssl


def connection_kwargs(
    config: BootstrapConfig,
    *,
    command_timeout: float | None = 30,
) -> dict[str, object]:
    return {
        "host": config.db_host,
        "port": config.db_port,
        "database": config.db_name,
        "user": config.db_user,
        "password": config.db_password,
        "command_timeout": command_timeout,
        "ssl": _ssl_value(config.db_ssl),
        "server_settings": {"TimeZone": "UTC"},
    }


def register_pool_config(pool: asyncpg.Pool, config: BootstrapConfig) -> None:
    _POOL_CONFIGS[id(pool)] = config


def unregister_pool_config(pool: asyncpg.Pool) -> None:
    _POOL_CONFIGS.pop(id(pool), None)


def pool_config(pool: asyncpg.Pool) -> BootstrapConfig | None:
    return _POOL_CONFIGS.get(id(pool))


async def create_pool(config: BootstrapConfig) -> asyncpg.Pool:
    pool = await asyncpg.create_pool(
        min_size=config.db_pool_min_size,
        max_size=config.db_pool_max_size,
        **connection_kwargs(config),
    )
    register_pool_config(pool, config)
    return pool


async def close_pool(pool: asyncpg.Pool) -> None:
    try:
        await pool.close()
    finally:
        unregister_pool_config(pool)
