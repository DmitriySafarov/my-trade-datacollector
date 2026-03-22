from __future__ import annotations

import asyncpg

from src.app_lifecycle import CollectorStartupError
from src.config.bootstrap import BootstrapConfig
from src.config.runtime import RuntimeSettings

from .base import BaseCollector


def build_runtime_collectors(
    *,
    config: BootstrapConfig,
    pool: asyncpg.Pool | None,
    runtime_settings: RuntimeSettings | None,
) -> list[BaseCollector]:
    del config, pool, runtime_settings
    raise CollectorStartupError("runtime collector factory is not wired")
