from __future__ import annotations

import asyncio
import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from src.collectors.hyperliquid import HyperliquidMarketCollector

from ._ws_test_support import (
    FakeHyperliquidSession,
    FakeHyperliquidSessionFactory,
    drain_session_callbacks,
)


FIXTURE_PATH = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "hyperliquid"
    / "ws_candle_messages.json"
)

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _load_fixture_messages() -> list[dict[str, object]]:
    return json.loads(FIXTURE_PATH.read_text())


async def _start_candle_collector(
    pool,
    *,
    count_limit: int = 50,
    time_limit_seconds: float = 60.0,
    reconnect_base_seconds: float = 1.0,
    reconnect_max_seconds: float = 30.0,
    reconnect_jitter_ratio: float = 0.2,
    candle_intervals: tuple[str, ...] | None = None,
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
        enable_l2book=False,
        enable_asset_ctx=False,
        enable_candles=True,
        candle_intervals=candle_intervals,
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
            row_count = await connection.fetchval("SELECT count(*) FROM hl_candles")
        if row_count == count:
            return
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_candle_collector_flushes_buffered_rows_on_stop(
    migrated_db: dict[str, object],
) -> None:
    pool = migrated_db["pool"]
    collector, task, session, _factory = await _start_candle_collector(
        pool, candle_intervals=("1m", "5m")
    )
    messages = _load_fixture_messages()

    expected_subs = {
        "candle:eth,1m",
        "candle:eth,5m",
        "candle:btc,1m",
        "candle:btc,5m",
    }
    assert expected_subs.issubset(set(session.subscriptions))

    session.emit("candle:eth,1m", messages[0])
    session.emit("candle:btc,1m", messages[1])
    await drain_session_callbacks()

    await collector.stop()
    await asyncio.wait_for(task, timeout=1.0)

    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT time, source, coin, interval, open_time, close_time,
                   open, high, low, close, volume, trades_count,
                   is_closed, event_hash, payload, ingested_at
            FROM hl_candles
            ORDER BY coin
            """,
        )

    assert len(rows) == 2
    assert [row["coin"] for row in rows] == ["BTC", "ETH"]
    assert [row["source"] for row in rows] == ["hl_ws_candles", "hl_ws_candles"]
    assert [row["interval"] for row in rows] == ["1m", "1m"]

    eth_row = next(r for r in rows if r["coin"] == "ETH")
    assert eth_row["open"] == pytest.approx(2126.0)
    assert eth_row["high"] == pytest.approx(2130.0)
    assert eth_row["low"] == pytest.approx(2125.0)
    assert eth_row["close"] == pytest.approx(2127.5)
    assert eth_row["volume"] == pytest.approx(1234.567)
    assert eth_row["trades_count"] == 42
    assert eth_row["is_closed"] is True
    assert eth_row["open_time"] == EPOCH + timedelta(milliseconds=1711234500000)
    assert eth_row["close_time"] == EPOCH + timedelta(milliseconds=1711234559999)

    btc_row = next(r for r in rows if r["coin"] == "BTC")
    assert btc_row["open"] == pytest.approx(70375.0)
    assert btc_row["high"] == pytest.approx(70500.0)
    assert btc_row["low"] == pytest.approx(70350.0)
    assert btc_row["close"] == pytest.approx(70400.0)
    assert btc_row["volume"] == pytest.approx(56.789)
    assert btc_row["trades_count"] == 128
    assert btc_row["is_closed"] is True

    for row in rows:
        payload = json.loads(row["payload"])
        assert row["event_hash"]
        assert len(row["event_hash"]) == 64
        assert row["time"].tzinfo is not None
        assert row["ingested_at"].tzinfo is not None
        assert row["time"].utcoffset().total_seconds() == 0
        assert payload["s"].upper() == row["coin"]
        assert float(payload["o"]) == row["open"]


@pytest.mark.asyncio
async def test_candle_collector_deduplicates_replayed_messages_across_reconnect(
    migrated_db: dict[str, object],
) -> None:
    pool = migrated_db["pool"]
    collector, task, session, factory = await _start_candle_collector(
        pool,
        candle_intervals=("1m",),
        reconnect_base_seconds=0.01,
        reconnect_max_seconds=0.01,
        reconnect_jitter_ratio=0.0,
    )
    message = _load_fixture_messages()[0]

    session.emit("candle:eth,1m", message)
    session.disconnect("network_drop")

    replacement = await asyncio.wait_for(factory.next_session(), timeout=1.0)
    replacement.open()
    await collector.wait_ready()
    replacement.emit("candle:eth,1m", deepcopy(message))
    await drain_session_callbacks()

    await collector.stop()
    await asyncio.wait_for(task, timeout=1.0)

    async with pool.acquire() as connection:
        row_count = await connection.fetchval(
            """
            SELECT count(*)
            FROM hl_candles
            WHERE source = 'hl_ws_candles'
              AND coin = 'ETH'
            """,
        )

    assert row_count == 1


@pytest.mark.asyncio
async def test_candle_collector_flushes_rows_on_timer(
    migrated_db: dict[str, object],
) -> None:
    pool = migrated_db["pool"]
    collector, task, session, _factory = await _start_candle_collector(
        pool,
        time_limit_seconds=0.01,
        candle_intervals=("1m",),
    )

    session.emit("candle:eth,1m", _load_fixture_messages()[0])

    await asyncio.wait_for(_wait_for_rows(pool, 1), timeout=1.0)
    await collector.stop()
    await asyncio.wait_for(task, timeout=1.0)


@pytest.mark.asyncio
async def test_candle_collector_handles_open_candle(
    migrated_db: dict[str, object],
) -> None:
    """An open candle (T == t) should be stored with is_closed=False."""
    pool = migrated_db["pool"]
    collector, task, session, _factory = await _start_candle_collector(
        pool, candle_intervals=("5m",)
    )
    open_candle = _load_fixture_messages()[2]

    session.emit("candle:eth,5m", open_candle)
    await drain_session_callbacks()

    await collector.stop()
    await asyncio.wait_for(task, timeout=1.0)

    async with pool.acquire() as connection:
        row = await connection.fetchrow(
            "SELECT is_closed, volume, trades_count FROM hl_candles WHERE interval = '5m'"
        )

    assert row["is_closed"] is False
    assert row["volume"] == pytest.approx(0.0)
    assert row["trades_count"] == 0


@pytest.mark.asyncio
async def test_candle_collector_registers_all_22_subscriptions_by_default(
    migrated_db: dict[str, object],
) -> None:
    pool = migrated_db["pool"]
    collector, task, session, _factory = await _start_candle_collector(pool)

    expected_intervals = [
        "1m",
        "3m",
        "5m",
        "15m",
        "30m",
        "1h",
        "2h",
        "4h",
        "8h",
        "12h",
        "1d",
    ]
    expected_subs = set()
    for coin in ("eth", "btc"):
        for interval in expected_intervals:
            expected_subs.add(f"candle:{coin},{interval}")

    assert expected_subs == set(session.subscriptions)
    assert len(session.subscriptions) == 22

    await collector.stop()
    await asyncio.wait_for(task, timeout=1.0)
