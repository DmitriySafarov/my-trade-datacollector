from __future__ import annotations

import asyncio
import json
from copy import deepcopy
from datetime import timezone
from pathlib import Path

import asyncpg
import pytest

from src.collectors.hyperliquid import HyperliquidTradesCollector
from src.db.pool import close_pool

from ._ws_test_support import FakeHyperliquidSession, FakeHyperliquidSessionFactory


FIXTURE_PATH = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "hyperliquid"
    / "ws_trades_messages.json"
)


def _load_fixture_messages() -> list[dict[str, object]]:
    return json.loads(FIXTURE_PATH.read_text())


async def _start_collector(
    pool,
    *,
    count_limit: int = 50,
    time_limit_seconds: float = 60.0,
    reconnect_base_seconds: float = 1.0,
    reconnect_max_seconds: float = 30.0,
    reconnect_jitter_ratio: float = 0.2,
) -> tuple[
    HyperliquidTradesCollector,
    asyncio.Task[None],
    FakeHyperliquidSession,
    FakeHyperliquidSessionFactory,
]:
    factory = FakeHyperliquidSessionFactory()
    collector = HyperliquidTradesCollector(
        base_url="https://api.hyperliquid.xyz",
        pool=pool,
        count_limit=count_limit,
        time_limit_seconds=time_limit_seconds,
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
            row_count = await connection.fetchval("SELECT count(*) FROM hl_trades")
        if row_count == count:
            return
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_trades_collector_flushes_buffered_rows_on_stop(
    migrated_db: dict[str, object],
) -> None:
    pool = migrated_db["pool"]
    collector, task, session, _factory = await _start_collector(pool)
    messages = _load_fixture_messages()

    session.emit("trades:eth", messages[0])
    session.emit("trades:btc", messages[1])

    await collector.stop()
    await asyncio.wait_for(task, timeout=1.0)

    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT time, source, coin, side, price, size, hash, tid, users, payload, ingested_at
            FROM hl_trades
            ORDER BY coin
            """,
        )

    assert len(rows) == 2
    assert [row["coin"] for row in rows] == ["BTC", "ETH"]
    assert [row["source"] for row in rows] == ["hl_ws_trades", "hl_ws_trades"]
    assert [row["side"] for row in rows] == ["A", "B"]
    assert [row["tid"] for row in rows] == [91002, 91001]
    assert [row["price"] for row in rows] == [84200.1, 3021.5]
    assert [row["size"] for row in rows] == [0.05, 1.25]
    for row in rows:
        assert row["time"].tzinfo is not None
        assert row["time"].astimezone(timezone.utc).utcoffset().total_seconds() == 0
        assert row["ingested_at"].tzinfo is not None
        assert json.loads(row["payload"])["tid"] == row["tid"]
        assert len(json.loads(row["users"])) == 2


@pytest.mark.asyncio
async def test_trades_collector_deduplicates_replayed_trade_ids(
    migrated_db: dict[str, object],
) -> None:
    pool = migrated_db["pool"]
    collector, task, session, _factory = await _start_collector(pool)
    message = _load_fixture_messages()[0]
    replay = deepcopy(message)
    replay["data"][0]["time"] += 1

    session.emit("trades:eth", message)
    session.emit("trades:eth", replay)

    await collector.stop()
    await asyncio.wait_for(task, timeout=1.0)

    async with pool.acquire() as connection:
        row_count = await connection.fetchval(
            """
            SELECT count(*)
            FROM hl_trades
            WHERE source = 'hl_ws_trades'
              AND coin = 'ETH'
              AND tid = 91001
            """,
        )

    assert row_count == 1


@pytest.mark.asyncio
async def test_trades_collector_flushes_rows_on_timer(
    migrated_db: dict[str, object],
) -> None:
    pool = migrated_db["pool"]
    collector, task, session, _factory = await _start_collector(
        pool,
        time_limit_seconds=0.01,
    )
    session.emit("trades:eth", _load_fixture_messages()[0])

    await asyncio.wait_for(_wait_for_rows(pool, 1), timeout=1.0)
    await collector.stop()
    await asyncio.wait_for(task, timeout=1.0)


@pytest.mark.asyncio
async def test_trades_collector_stop_unblocks_when_final_flush_fails(
    migrated_db: dict[str, object],
) -> None:
    pool = migrated_db["pool"]
    collector, task, session, _factory = await _start_collector(pool)
    session.emit("trades:eth", _load_fixture_messages()[0])
    await close_pool(pool)

    await asyncio.wait_for(collector.stop(), timeout=1.0)
    with pytest.raises(asyncpg.InterfaceError, match="closed"):
        await asyncio.wait_for(task, timeout=1.0)
