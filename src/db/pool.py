from __future__ import annotations

import asyncpg

from src.config.bootstrap import BootstrapConfig


def _ssl_value(db_ssl: str) -> str | None:
    return None if db_ssl == "disable" else db_ssl


async def create_pool(config: BootstrapConfig) -> asyncpg.Pool:
    return await asyncpg.create_pool(
        host=config.db_host,
        port=config.db_port,
        database=config.db_name,
        user=config.db_user,
        password=config.db_password,
        min_size=config.db_pool_min_size,
        max_size=config.db_pool_max_size,
        command_timeout=30,
        ssl=_ssl_value(config.db_ssl),
    )
