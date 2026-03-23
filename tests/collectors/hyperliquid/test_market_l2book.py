from __future__ import annotations

import asyncio
import json
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path

import asyncpg
import pytest

from src.collectors.hyperliquid import HyperliquidMarketCollector
from src.db.pool import close_pool

from ._ws_test_support import (
    FakeHyperliquidSession,
    FakeHyperliquidSessionFactory,
    drain_session_callbacks,
)


FIXTURE_PATH = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "hyperliquid"
    / "ws_l2book_messages.json"
)


def _load_fixture_messages() -> list[dict[str, object]]:
    return json.loads(FIXTURE_PATH.read_text())


async def _start_l2book_collector(
    pool,
    *,
    count_limit: int = 50,
    time_limit_seconds: float = 60.0,
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
        count_limit=count_limit,
        time_limit_seconds=time_limit_seconds,
        enable_trades=False,
        enable_l2book=True,
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


async def _wait_for_rows(pool, count: int) -> None:
    while True:
        async with pool.acquire() as connection:
            row_count = await connection.fetchval("SELECT count(*) FROM hl_l2book")
        if row_count == count:
            return
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_l2book_collector_flushes_buffered_rows_on_stop(
    migrated_db: dict[str, object],
) -> None:
    pool = migrated_db["pool"]
    collector, task, session, _factory = await _start_l2book_collector(pool)
    messages = _load_fixture_messages()

    session.emit("l2Book:eth", messages[0])
    session.emit("l2Book:btc", messages[1])
    await drain_session_callbacks()

    await collector.stop()
    await asyncio.wait_for(task, timeout=1.0)

    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT time, source, coin, bids, asks, snapshot_hash, best_bid_price,
                   best_bid_size, best_ask_price, best_ask_size, spread,
                   bid_notional, ask_notional, payload, ingested_at
            FROM hl_l2book
            ORDER BY coin
            """,
        )

    assert len(rows) == 2
    assert [row["coin"] for row in rows] == ["BTC", "ETH"]
    assert [row["source"] for row in rows] == ["hl_ws_l2book", "hl_ws_l2book"]
    assert [row["best_bid_price"] for row in rows] == [65000.5, 3021.5]
    assert [row["best_ask_price"] for row in rows] == [65001.0, 3022.0]
    assert [row["best_bid_size"] for row in rows] == [1.25, 8.2]
    assert [row["best_ask_size"] for row in rows] == [0.9, 6.1]
    assert [row["spread"] for row in rows] == [0.5, 0.5]
    assert [row["bid_notional"] for row in rows] == pytest.approx([133249.825, 35349.8])
    assert [row["ask_notional"] for row in rows] == pytest.approx([84501.9, 25688.2])
    assert [row["time"] for row in rows] == [
        datetime.fromtimestamp(1710000000.456, tz=UTC),
        datetime.fromtimestamp(1710000000.123, tz=UTC),
    ]
    for row in rows:
        assert row["snapshot_hash"]
        assert len(row["snapshot_hash"]) == 64
        assert row["ingested_at"].tzinfo is not None
        assert row["time"].tzinfo is not None
        assert json.loads(row["payload"])["coin"] == row["coin"]
        assert float(json.loads(row["bids"])[0]["px"]) == row["best_bid_price"]
        assert float(json.loads(row["asks"])[0]["px"]) == row["best_ask_price"]


@pytest.mark.asyncio
async def test_l2book_collector_deduplicates_replayed_snapshots(
    migrated_db: dict[str, object],
) -> None:
    pool = migrated_db["pool"]
    collector, task, session, _factory = await _start_l2book_collector(pool)
    message = _load_fixture_messages()[0]

    session.emit("l2Book:eth", message)
    session.emit("l2Book:eth", deepcopy(message))
    await drain_session_callbacks()

    await collector.stop()
    await asyncio.wait_for(task, timeout=1.0)

    async with pool.acquire() as connection:
        row_count = await connection.fetchval(
            """
            SELECT count(*)
            FROM hl_l2book
            WHERE source = 'hl_ws_l2book'
              AND coin = 'ETH'
              AND best_bid_price = 3021.5
            """,
        )

    assert row_count == 1


@pytest.mark.asyncio
async def test_l2book_collector_flushes_rows_on_timer(
    migrated_db: dict[str, object],
) -> None:
    pool = migrated_db["pool"]
    collector, task, session, _factory = await _start_l2book_collector(
        pool,
        time_limit_seconds=0.01,
    )
    session.emit("l2Book:eth", _load_fixture_messages()[0])

    await asyncio.wait_for(_wait_for_rows(pool, 1), timeout=1.0)
    await collector.stop()
    await asyncio.wait_for(task, timeout=1.0)


@pytest.mark.asyncio
async def test_l2book_collector_stop_unblocks_when_final_flush_fails(
    migrated_db: dict[str, object],
) -> None:
    pool = migrated_db["pool"]
    collector, task, session, _factory = await _start_l2book_collector(pool)
    session.emit("l2Book:eth", _load_fixture_messages()[0])
    await close_pool(pool)

    await asyncio.wait_for(collector.stop(), timeout=1.0)
    with pytest.raises(asyncpg.InterfaceError, match="closed"):
        await asyncio.wait_for(task, timeout=1.0)
