from __future__ import annotations

import os
from dataclasses import dataclass


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value is not None else default


def _get_float(name: str, default: float) -> float:
    value = os.getenv(name)
    return float(value) if value is not None else default


@dataclass(frozen=True)
class BootstrapConfig:
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    db_ssl: str
    service_name: str
    log_level: str
    health_host: str
    health_port: int
    db_pool_min_size: int
    db_pool_max_size: int
    settings_refresh_seconds: int
    default_batch_count: int
    default_batch_seconds: float
    shutdown_timeout_seconds: int
    finnhub_api_key: str | None
    fred_api_key: str | None
    binance_rest_base_url: str
    hyperliquid_api_url: str


def load_bootstrap_config() -> BootstrapConfig:
    return BootstrapConfig(
        db_host=os.getenv("DB_HOST", "127.0.0.1"),
        db_port=_get_int("DB_PORT", 55432),
        db_name=os.getenv("DB_NAME", "collector"),
        db_user=os.getenv("DB_USER", "collector"),
        db_password=os.getenv("DB_PASSWORD", "collector"),
        db_ssl=os.getenv("DB_SSL", "disable"),
        service_name=os.getenv("COLLECTOR_SERVICE_NAME", "data-collector"),
        log_level=os.getenv("COLLECTOR_LOG_LEVEL", "INFO"),
        health_host=os.getenv("COLLECTOR_HEALTH_HOST", "0.0.0.0"),
        health_port=_get_int("COLLECTOR_HEALTH_PORT", 8080),
        db_pool_min_size=_get_int("COLLECTOR_DB_POOL_MIN_SIZE", 2),
        db_pool_max_size=_get_int("COLLECTOR_DB_POOL_MAX_SIZE", 10),
        settings_refresh_seconds=_get_int("COLLECTOR_SETTINGS_REFRESH_SECONDS", 30),
        default_batch_count=_get_int("COLLECTOR_DEFAULT_BATCH_COUNT", 500),
        default_batch_seconds=_get_float("COLLECTOR_DEFAULT_BATCH_SECONDS", 2.0),
        shutdown_timeout_seconds=_get_int("COLLECTOR_SHUTDOWN_TIMEOUT_SECONDS", 15),
        finnhub_api_key=os.getenv("FINNHUB_API_KEY") or None,
        fred_api_key=os.getenv("FRED_API_KEY") or None,
        binance_rest_base_url=os.getenv(
            "BINANCE_REST_BASE_URL",
            "https://fapi.binance.com",
        ),
        hyperliquid_api_url=os.getenv(
            "HYPERLIQUID_API_URL",
            "https://api.hyperliquid.xyz",
        ),
    )
