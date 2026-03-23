from __future__ import annotations

import asyncpg

from src.app_lifecycle import CollectorStartupError
from src.config.bootstrap import BootstrapConfig
from src.config.runtime import RuntimeSettings

from .base import BaseCollector
from .hyperliquid import HyperliquidTradesCollector


def build_runtime_collectors(
    *,
    config: BootstrapConfig,
    pool: asyncpg.Pool | None,
    runtime_settings: RuntimeSettings | None,
) -> list[BaseCollector]:
    if pool is None or runtime_settings is None:
        raise CollectorStartupError(
            "runtime collector factory requires initialized pool and runtime settings"
        )
    return [
        HyperliquidTradesCollector(
            base_url=config.hyperliquid_api_url,
            pool=pool,
            count_limit=runtime_settings.default_batch_count,
            time_limit_seconds=runtime_settings.default_batch_seconds,
        )
    ]
