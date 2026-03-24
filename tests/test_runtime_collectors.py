from __future__ import annotations

from src.collectors import build_runtime_collectors
from src.collectors.hyperliquid import HyperliquidMarketCollector
from src.config.runtime import RuntimeSettings
from tests._db_test_support import _test_config


def test_runtime_collector_factory_builds_hyperliquid_market_collector() -> None:
    collectors = build_runtime_collectors(
        config=_test_config("collector"),
        pool=object(),
        runtime_settings=RuntimeSettings(
            settings_refresh_seconds=30,
            default_batch_count=123,
            default_batch_seconds=4.5,
        ),
    )

    assert len(collectors) == 1
    collector = collectors[0]
    assert isinstance(collector, HyperliquidMarketCollector)
    assert collector.source_ids == (
        "hl_ws_trades",
        "hl_ws_l2book",
        "hl_ws_asset_ctx",
    )
    assert collector.health_snapshot()["writers"] == {
        "hl_ws_trades": {
            "name": "hyperliquid_trades_writer",
            "buffer_size": 0,
            "count_limit": 123,
            "time_limit_seconds": 4.5,
            "last_error": "",
        },
        "hl_ws_l2book": {
            "name": "hyperliquid_l2book_writer",
            "buffer_size": 0,
            "count_limit": 123,
            "time_limit_seconds": 4.5,
            "last_error": "",
        },
        "hl_ws_asset_ctx": {
            "name": "hyperliquid_asset_ctx_writer",
            "buffer_size": 0,
            "count_limit": 123,
            "time_limit_seconds": 4.5,
            "last_error": "",
        },
    }
