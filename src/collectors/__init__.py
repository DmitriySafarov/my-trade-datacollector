from __future__ import annotations

import asyncpg

from src.app_lifecycle import CollectorStartupError
from src.config.bootstrap import BootstrapConfig
from src.config.runtime import RuntimeSettings

from .base import BaseCollector
from .hyperliquid import HyperliquidMarketCollector
from .rest import HyperliquidCandlesPoller, HyperliquidFundingPoller
from .rest.rate_limiter import SlidingWindowRateLimiter


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
    # Shared rate limiter for all REST pollers hitting the Hyperliquid API.
    hl_rest_limiter = SlidingWindowRateLimiter(max_weight=1200, window_seconds=60)
    return [
        HyperliquidMarketCollector(
            base_url=config.hyperliquid_api_url,
            pool=pool,
            count_limit=runtime_settings.default_batch_count,
            time_limit_seconds=runtime_settings.default_batch_seconds,
            enable_asset_ctx=True,
            enable_candles=True,
        ),
        HyperliquidCandlesPoller(
            base_url=config.hyperliquid_api_url,
            pool=pool,
            rate_limiter=hl_rest_limiter,
        ),
        HyperliquidFundingPoller(
            base_url=config.hyperliquid_api_url,
            pool=pool,
            rate_limiter=hl_rest_limiter,
        ),
    ]
