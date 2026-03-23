from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import UTC, datetime

import pytest

from src.collectors.hyperliquid import HyperliquidMarketCollector

from .test_market_l2book import _load_fixture_messages
from .test_trades_collector import _load_fixture_messages as _load_trade_messages
from ._ws_test_support import (
    FakeHyperliquidSession,
    FakeHyperliquidSessionFactory,
    drain_session_callbacks,
)


async def _start_market_collector(
    pool,
    *,
    reconnect_base_seconds: float = 1.0,
    reconnect_max_seconds: float = 30.0,
    reconnect_jitter_ratio: float = 0.2,
) -> tuple[
    HyperliquidMarketCollector,
    asyncio.Task[None],
    FakeHyperliquidSession,
    FakeHyperliquidSessionFactory,
]:
    factory = FakeHyperliquidSessionFactory()
    collector = HyperliquidMarketCollector(
        base_url="https://api.hyperliquid.xyz",
        pool=pool,
        count_limit=50,
        time_limit_seconds=60.0,
        reconnect_base_seconds=reconnect_base_seconds,
        reconnect_max_seconds=reconnect_max_seconds,
        reconnect_jitter_ratio=reconnect_jitter_ratio,
        session_factory=factory,
    )
    task = asyncio.create_task(collector.start())
    session = await factory.next_session()
    session.open()
    await collector.wait_ready()
    return collector, task, session, factory


@pytest.mark.asyncio
async def test_market_collector_uses_one_session_for_trades_and_l2book_across_reconnect(
    migrated_db: dict[str, object],
) -> None:
    pool = migrated_db["pool"]
    collector, task, session, factory = await _start_market_collector(
        pool,
        reconnect_base_seconds=0.01,
        reconnect_max_seconds=0.01,
        reconnect_jitter_ratio=0.0,
    )
    trade_message = _load_trade_messages()[0]
    trade_replay = deepcopy(trade_message)
    trade_resumed = deepcopy(trade_message)
    trade_resumed["data"][0]["tid"] = 91003
    trade_resumed["data"][0]["hash"] = "0xethtrade003"
    trade_resumed["data"][0]["time"] += 10
    book_message = _load_fixture_messages()[0]
    book_replay = deepcopy(book_message)
    book_resumed = deepcopy(book_message)
    book_resumed["data"]["time"] += 1

    assert set(session.subscriptions) == {
        "trades:eth",
        "trades:btc",
        "l2Book:eth",
        "l2Book:btc",
    }

    session.emit("trades:eth", trade_message)
    session.emit("l2Book:eth", book_message)
    session.disconnect("network_drop")

    replacement = await asyncio.wait_for(factory.next_session(), timeout=1.0)
    replacement.open()
    await collector.wait_ready()
    replacement.emit("trades:eth", trade_replay)
    replacement.emit("trades:eth", trade_resumed)
    replacement.emit("l2Book:eth", book_replay)
    replacement.emit("l2Book:eth", book_resumed)
    await drain_session_callbacks()

    await collector.stop()
    await asyncio.wait_for(task, timeout=1.0)

    async with pool.acquire() as connection:
        trade_rows = await connection.fetch(
            """
            SELECT tid
            FROM hl_trades
            WHERE source = 'hl_ws_trades'
              AND coin = 'ETH'
            ORDER BY tid
            """,
        )
        book_rows = await connection.fetch(
            """
            SELECT time, best_bid_price
            FROM hl_l2book
            WHERE source = 'hl_ws_l2book'
              AND coin = 'ETH'
            ORDER BY time
            """,
        )

    assert [row["tid"] for row in trade_rows] == [91001, 91003]
    assert [row["best_bid_price"] for row in book_rows] == [3021.5, 3021.5]
    assert [row["time"] for row in book_rows] == [
        datetime.fromtimestamp(1710000000.123, tz=UTC),
        datetime.fromtimestamp(1710000000.124, tz=UTC),
    ]


@pytest.mark.asyncio
async def test_market_collector_health_tracks_sources_independently(
    migrated_db: dict[str, object],
) -> None:
    pool = migrated_db["pool"]
    collector, task, session, factory = await _start_market_collector(
        pool,
        reconnect_base_seconds=0.01,
        reconnect_max_seconds=0.01,
        reconnect_jitter_ratio=0.0,
    )

    session.emit("l2Book:eth", _load_fixture_messages()[0])
    await asyncio.wait_for(
        _wait_for(
            lambda: (
                collector.health_snapshot()["source_health"]["hl_ws_l2book"][
                    "last_message_at"
                ]
                is not None
            )
        ),
        timeout=1.0,
    )

    snapshot = collector.health_snapshot()
    assert snapshot["source_health"] == snapshot["sources"]
    assert snapshot["source_health"]["hl_ws_trades"]["last_message_at"] is None
    assert snapshot["source_health"]["hl_ws_l2book"]["last_message_at"] is not None

    session.disconnect("network_drop")
    await asyncio.wait_for(
        _wait_for(
            lambda: collector.health_snapshot()["current_gap_started_at"] is not None
        ),
        timeout=1.0,
    )

    gap_snapshot = collector.health_snapshot()
    assert gap_snapshot["current_gap_started_at"] is not None
    assert (
        gap_snapshot["source_health"]["hl_ws_trades"]["current_gap_started_at"] is None
    )
    assert (
        gap_snapshot["source_health"]["hl_ws_l2book"]["current_gap_started_at"] is None
    )

    replacement = await asyncio.wait_for(factory.next_session(), timeout=1.0)
    replacement.open()
    await collector.wait_ready()

    await collector.stop()
    await asyncio.wait_for(task, timeout=1.0)


async def _wait_for(predicate, *, interval: float = 0.01) -> None:
    while not predicate():
        await asyncio.sleep(interval)
