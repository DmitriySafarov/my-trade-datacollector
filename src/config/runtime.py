from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import asyncpg

from src.config.bootstrap import BootstrapConfig


@dataclass(frozen=True)
class RuntimeSettings:
    settings_refresh_seconds: int
    default_batch_count: int
    default_batch_seconds: float


def _coerce_int(values: dict[str, Any], key: str, fallback: int) -> int:
    value = values.get(key, fallback)
    return int(value)


def _coerce_float(values: dict[str, Any], key: str, fallback: float) -> float:
    value = values.get(key, fallback)
    return float(value)


async def load_runtime_settings(
    pool: asyncpg.Pool,
    bootstrap: BootstrapConfig,
) -> RuntimeSettings:
    query = """
        SELECT key, value
        FROM settings
    """
    async with pool.acquire() as connection:
        rows = await connection.fetch(query)

    values = {row["key"]: row["value"] for row in rows}
    return RuntimeSettings(
        settings_refresh_seconds=_coerce_int(
            values,
            "settings.refresh_seconds",
            bootstrap.settings_refresh_seconds,
        ),
        default_batch_count=_coerce_int(
            values,
            "batch.default.count",
            bootstrap.default_batch_count,
        ),
        default_batch_seconds=_coerce_float(
            values,
            "batch.default.seconds",
            bootstrap.default_batch_seconds,
        ),
    )
