from __future__ import annotations

import asyncio
import json
from copy import deepcopy
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
    / "ws_asset_ctx_messages.json"
)


def _load_fixture_messages() -> list[dict[str, object]]:
    return json.loads(FIXTURE_PATH.read_text())


async def _start_asset_ctx_collector(
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
        enable_l2book=False,
        enable_asset_ctx=True,
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
            row_count = await connection.fetchval("SELECT count(*) FROM hl_asset_ctx")
        if row_count == count:
            return
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_asset_ctx_collector_flushes_buffered_rows_on_stop(
    migrated_db: dict[str, object],
) -> None:
    pool = migrated_db["pool"]
    collector, task, session, _factory = await _start_asset_ctx_collector(pool)
    messages = _load_fixture_messages()

    assert set(session.subscriptions) == {"activeAssetCtx:eth", "activeAssetCtx:btc"}

    session.emit("activeAssetCtx:eth", messages[0])
    session.emit("activeAssetCtx:btc", messages[1])
    await drain_session_callbacks()

    await collector.stop()
    await asyncio.wait_for(task, timeout=1.0)

    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT time, source, coin, funding, open_interest, mark_price, oracle_price,
                   mid_price, premium, buy_impact_price, sell_impact_price,
                   day_notional_volume, prev_day_price, event_hash, payload, ingested_at
            FROM hl_asset_ctx
            ORDER BY coin
            """,
        )

    assert len(rows) == 2
    assert [row["coin"] for row in rows] == ["BTC", "ETH"]
    assert [row["source"] for row in rows] == ["hl_ws_asset_ctx", "hl_ws_asset_ctx"]
    assert [row["funding"] for row in rows] == pytest.approx(
        [0.0000038498, -0.0000001002]
    )
    assert [row["open_interest"] for row in rows] == pytest.approx(
        [25559.40104, 553194.1281999996]
    )
    assert [row["mark_price"] for row in rows] == pytest.approx([70388.0, 2127.2])
    assert [row["oracle_price"] for row in rows] == pytest.approx([70426.0, 2128.3])
    assert [row["mid_price"] for row in rows] == pytest.approx([70384.5, 2127.35])
    assert [row["premium"] for row in rows] == pytest.approx(
        [-0.0005821714, -0.0004228727]
    )
    assert [row["buy_impact_price"] for row in rows] == pytest.approx([70366.9, 2127.3])
    assert [row["sell_impact_price"] for row in rows] == pytest.approx(
        [70385.0, 2127.4]
    )
    assert [row["day_notional_volume"] for row in rows] == pytest.approx(
        [3946742275.1811518669, 1756640673.5487046242]
    )
    assert [row["prev_day_price"] for row in rows] == pytest.approx([68696.0, 2078.8])
    for row in rows:
        payload = json.loads(row["payload"])
        assert row["event_hash"]
        assert len(row["event_hash"]) == 64
        assert row["time"].tzinfo is not None
        assert row["ingested_at"].tzinfo is not None
        assert row["time"].utcoffset().total_seconds() == 0
        assert payload["coin"] == row["coin"]
        assert float(payload["ctx"]["markPx"]) == row["mark_price"]


@pytest.mark.asyncio
async def test_asset_ctx_collector_deduplicates_replayed_messages_across_reconnect(
    migrated_db: dict[str, object],
) -> None:
    pool = migrated_db["pool"]
    collector, task, session, factory = await _start_asset_ctx_collector(
        pool,
        reconnect_base_seconds=0.01,
        reconnect_max_seconds=0.01,
        reconnect_jitter_ratio=0.0,
    )
    message = _load_fixture_messages()[0]

    session.emit("activeAssetCtx:eth", message)
    session.disconnect("network_drop")

    replacement = await asyncio.wait_for(factory.next_session(), timeout=1.0)
    replacement.open()
    await collector.wait_ready()
    replacement.emit("activeAssetCtx:eth", deepcopy(message))
    await drain_session_callbacks()

    await collector.stop()
    await asyncio.wait_for(task, timeout=1.0)

    async with pool.acquire() as connection:
        row_count = await connection.fetchval(
            """
            SELECT count(*)
            FROM hl_asset_ctx
            WHERE source = 'hl_ws_asset_ctx'
              AND coin = 'ETH'
            """,
        )

    assert row_count == 1


@pytest.mark.asyncio
async def test_asset_ctx_collector_flushes_rows_on_timer(
    migrated_db: dict[str, object],
) -> None:
    pool = migrated_db["pool"]
    collector, task, session, _factory = await _start_asset_ctx_collector(
        pool,
        time_limit_seconds=0.01,
    )

    session.emit("activeAssetCtx:eth", _load_fixture_messages()[0])

    await asyncio.wait_for(_wait_for_rows(pool, 1), timeout=1.0)
    await collector.stop()
    await asyncio.wait_for(task, timeout=1.0)
