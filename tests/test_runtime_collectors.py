from __future__ import annotations

from unittest.mock import MagicMock

import asyncpg

from src.collectors import build_runtime_collectors
from src.collectors.binance import (
    BinanceAggTradeCollector,
    BinanceDepthCollector,
    BinanceMarkPriceCollector,
)
from src.collectors.hyperliquid import HyperliquidMarketCollector
from src.collectors.rest import HyperliquidCandlesPoller, HyperliquidFundingPoller
from src.config.runtime import RuntimeSettings
from tests._db_test_support import _test_config


def test_runtime_collector_factory_builds_all_collectors() -> None:
    # Exception to project no-mocks convention: the factory only stores the
    # pool reference and never calls it, so a spec-constrained mock is
    # sufficient and avoids the overhead of spinning up a real DB fixture.
    collectors = build_runtime_collectors(
        config=_test_config("collector"),
        pool=MagicMock(spec=asyncpg.Pool),
        runtime_settings=RuntimeSettings(
            settings_refresh_seconds=30,
            default_batch_count=123,
            default_batch_seconds=4.5,
        ),
    )

    assert len(collectors) == 6

    ws_collector = collectors[0]
    assert isinstance(ws_collector, HyperliquidMarketCollector)
    assert ws_collector.source_ids == (
        "hl_ws_trades",
        "hl_ws_l2book",
        "hl_ws_asset_ctx",
        "hl_ws_candles",
    )
    assert ws_collector.health_snapshot()["writers"] == {
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
        "hl_ws_candles": {
            "name": "hyperliquid_candles_writer",
            "buffer_size": 0,
            "count_limit": 123,
            "time_limit_seconds": 4.5,
            "last_error": "",
        },
    }

    rest_candles = collectors[1]
    assert isinstance(rest_candles, HyperliquidCandlesPoller)
    assert rest_candles.source_ids == ("hl_rest_candles",)
    assert rest_candles.name == "hl_rest_candles"
    assert rest_candles._rate_limiter is not None
    assert rest_candles._rate_limiter.max_weight == 1200

    rest_funding = collectors[2]
    assert isinstance(rest_funding, HyperliquidFundingPoller)
    assert rest_funding.source_ids == ("hl_rest_funding",)
    assert rest_funding.name == "hl_rest_funding"
    assert rest_funding._rate_limiter is not None
    # Shared rate limiter between all HL REST pollers
    assert rest_funding._rate_limiter is rest_candles._rate_limiter

    bn_agg_trades = collectors[3]
    assert isinstance(bn_agg_trades, BinanceAggTradeCollector)
    assert bn_agg_trades.source_ids == ("bn_ws_agg_trades",)
    assert bn_agg_trades.name == "binance_agg_trades"

    bn_mark_price = collectors[4]
    assert isinstance(bn_mark_price, BinanceMarkPriceCollector)
    assert bn_mark_price.source_ids == ("bn_ws_mark_price",)
    assert bn_mark_price.name == "binance_mark_price"

    bn_depth = collectors[5]
    assert isinstance(bn_depth, BinanceDepthCollector)
    assert bn_depth.source_ids == ("bn_ws_depth",)
    assert bn_depth.name == "binance_depth"
