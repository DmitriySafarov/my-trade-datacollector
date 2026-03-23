from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from src.collectors.hyperliquid import HyperliquidMarketCollector

from ._ws_test_support import FakeHyperliquidSessionFactory
from .test_market_l2book import _load_fixture_messages


TRADE_FIXTURE_PATH = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "hyperliquid"
    / "ws_trades_messages.json"
)


def _load_trade_message() -> dict[str, object]:
    return json.loads(TRADE_FIXTURE_PATH.read_text())[0]


@pytest.mark.asyncio
async def test_market_collector_health_reports_per_source_freshness_and_gap(
    migrated_db: dict[str, object],
) -> None:
    pool = migrated_db["pool"]
    factory = FakeHyperliquidSessionFactory()
    collector = HyperliquidMarketCollector(
        base_url="https://api.hyperliquid.xyz",
        pool=pool,
        count_limit=50,
        time_limit_seconds=60.0,
        enable_trades=True,
        enable_l2book=True,
        reconnect_base_seconds=0.01,
        reconnect_max_seconds=0.01,
        reconnect_jitter_ratio=0.0,
        session_factory=factory,
    )
    task = asyncio.create_task(collector.start())
    first = await asyncio.wait_for(factory.next_session(), timeout=1.0)

    first.open()
    await collector.wait_ready()
    first.emit("trades:eth", _load_trade_message())
    await asyncio.wait_for(
        _wait_for(
            lambda: (
                collector.health_snapshot()["sources"]["hl_ws_trades"][
                    "last_message_at"
                ]
                is not None
            )
        ),
        timeout=1.0,
    )

    snapshot = collector.health_snapshot()
    assert snapshot["sources"]["hl_ws_trades"]["last_message_at"] is not None
    assert snapshot["sources"]["hl_ws_l2book"]["last_message_at"] is None

    first.emit("l2Book:eth", _load_fixture_messages()[0])
    await asyncio.wait_for(
        _wait_for(
            lambda: (
                collector.health_snapshot()["sources"]["hl_ws_l2book"][
                    "last_message_at"
                ]
                is not None
            )
        ),
        timeout=1.0,
    )

    first.disconnect("network_drop")
    second = await asyncio.wait_for(factory.next_session(), timeout=1.0)
    second.open()
    await collector.wait_ready()

    snapshot = collector.health_snapshot()
    assert snapshot["last_gap_started_at"] is not None
    assert snapshot["last_gap_ended_at"] is not None
    for source_id in ("hl_ws_trades", "hl_ws_l2book"):
        assert snapshot["sources"][source_id]["last_gap_started_at"] is None
        assert snapshot["sources"][source_id]["last_gap_ended_at"] is None

    await collector.stop()
    await asyncio.wait_for(task, timeout=1.0)


async def _wait_for(predicate, *, interval: float = 0.01) -> None:
    while not predicate():
        await asyncio.sleep(interval)
