"""DB-backed integration tests for BinanceAggTradeCollector."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import aiohttp
import pytest

from src.collectors.binance.agg_trade_collector import BinanceAggTradeCollector
from src.collectors.binance.agg_trade_parsing import SOURCE_ID

FIXTURE_PATH = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "binance"
    / "ws_agg_trade_messages.json"
)


def _load_fixtures() -> list[dict[str, Any]]:
    return json.loads(FIXTURE_PATH.read_text())


# ---------------------------------------------------------------------------
# WS mock helpers (similar to binance/test_ws_manager.py)
# ---------------------------------------------------------------------------


@dataclass
class _Msg:
    type: aiohttp.WSMsgType
    data: str | int | None = None


class _FakeWs:
    """Fake WS that yields messages then blocks until closed."""

    def __init__(self, messages: list[dict[str, Any]]) -> None:
        self._raw = [_Msg(aiohttp.WSMsgType.TEXT, json.dumps(m)) for m in messages]
        self._idx = 0
        self.closed = False
        self._close_event = asyncio.Event()

    def __aiter__(self) -> _FakeWs:
        return self

    async def __anext__(self) -> _Msg:
        if self._idx < len(self._raw):
            msg = self._raw[self._idx]
            self._idx += 1
            return msg
        # Block until closed
        await self._close_event.wait()
        raise StopAsyncIteration

    async def close(self) -> None:
        self.closed = True
        self._close_event.set()


def _mock_session(ws: _FakeWs) -> AsyncMock:
    session = AsyncMock()
    session.ws_connect = AsyncMock(return_value=ws)
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=session)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return ctx


async def _wait_for_rows(pool: Any, count: int, timeout: float = 5.0) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        async with pool.acquire() as conn:
            n = await conn.fetchval("SELECT count(*) FROM bn_agg_trades")
        if n >= count:
            return
        await asyncio.sleep(0.02)
    raise TimeoutError(f"Expected {count} rows, got {n}")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_collector_writes_agg_trades_on_stop(
    migrated_db: dict[str, object],
) -> None:
    """Messages received before stop() are flushed to bn_agg_trades."""
    pool = migrated_db["pool"]
    fixtures = _load_fixtures()
    ws = _FakeWs(fixtures)

    collector = BinanceAggTradeCollector(
        pool=pool,
        count_limit=50,
        time_limit_seconds=60.0,
    )
    with patch("aiohttp.ClientSession", return_value=_mock_session(ws)):
        task = asyncio.create_task(collector.start())
        await asyncio.wait_for(collector.wait_ready(), timeout=5.0)
        # Allow messages to be processed
        await asyncio.sleep(0.1)
        await collector.stop()
        await asyncio.wait_for(task, timeout=5.0)

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT time, source, symbol, trade_id, agg_trade_id,
                   first_trade_id, last_trade_id, price, qty,
                   is_buyer_maker, is_best_match, payload, ingested_at
            FROM bn_agg_trades
            ORDER BY symbol
            """
        )

    assert len(rows) == 2
    assert [r["symbol"] for r in rows] == ["BTCUSDT", "ETHUSDT"]
    assert [r["source"] for r in rows] == [SOURCE_ID, SOURCE_ID]
    assert [r["agg_trade_id"] for r in rows] == [789012345, 456789012]
    assert [r["price"] for r in rows] == [84200.10, 3021.50]
    assert [r["qty"] for r in rows] == [0.050, 1.250]
    assert [r["is_buyer_maker"] for r in rows] == [True, False]
    # trade_id and is_best_match should be NULL for aggTrade
    for row in rows:
        assert row["trade_id"] is None
        assert row["is_best_match"] is None
        assert row["time"].tzinfo is not None
        assert row["ingested_at"].tzinfo is not None
        payload = json.loads(row["payload"])
        assert payload["a"] == row["agg_trade_id"]


@pytest.mark.asyncio
async def test_collector_deduplicates_replayed_agg_trade_ids(
    migrated_db: dict[str, object],
) -> None:
    """Replay guard trigger rejects duplicate agg_trade_id for same source+time+symbol."""
    pool = migrated_db["pool"]
    fixture = _load_fixtures()[0]  # ETH message
    # Send the exact same message twice
    ws = _FakeWs([fixture, fixture])

    collector = BinanceAggTradeCollector(
        pool=pool,
        count_limit=50,
        time_limit_seconds=60.0,
    )
    with patch("aiohttp.ClientSession", return_value=_mock_session(ws)):
        task = asyncio.create_task(collector.start())
        await asyncio.wait_for(collector.wait_ready(), timeout=5.0)
        await asyncio.sleep(0.1)
        await collector.stop()
        await asyncio.wait_for(task, timeout=5.0)

    async with pool.acquire() as conn:
        count = await conn.fetchval(
            """
            SELECT count(*) FROM bn_agg_trades
            WHERE source = $1 AND symbol = 'ETHUSDT' AND agg_trade_id = 456789012
            """,
            SOURCE_ID,
        )
    # Unique index on (source, time, symbol, agg_trade_id) prevents exact duplicates
    assert count == 1


@pytest.mark.asyncio
async def test_collector_flushes_on_timer(
    migrated_db: dict[str, object],
) -> None:
    """Rows are flushed by timer before stop()."""
    pool = migrated_db["pool"]
    fixtures = _load_fixtures()
    ws = _FakeWs(fixtures[:1])

    collector = BinanceAggTradeCollector(
        pool=pool,
        count_limit=50,
        time_limit_seconds=0.05,
    )
    with patch("aiohttp.ClientSession", return_value=_mock_session(ws)):
        task = asyncio.create_task(collector.start())
        await asyncio.wait_for(collector.wait_ready(), timeout=5.0)
        await _wait_for_rows(pool, 1)
        await collector.stop()
        await asyncio.wait_for(task, timeout=5.0)


@pytest.mark.asyncio
async def test_collector_health_snapshot(
    migrated_db: dict[str, object],
) -> None:
    """Health snapshot includes name, source_ids, and writer state."""
    pool = migrated_db["pool"]
    ws = _FakeWs([])

    collector = BinanceAggTradeCollector(
        pool=pool,
        count_limit=50,
        time_limit_seconds=60.0,
    )
    with patch("aiohttp.ClientSession", return_value=_mock_session(ws)):
        task = asyncio.create_task(collector.start())
        await asyncio.wait_for(collector.wait_ready(), timeout=5.0)
        snap = collector.health_snapshot()
        await collector.stop()
        await asyncio.wait_for(task, timeout=5.0)

    assert snap["name"] == "binance_agg_trades"
    assert snap["source_ids"] == [SOURCE_ID]
    assert "writer" in snap
    assert snap["writer"]["name"] == "binance_agg_trades_writer"


@pytest.mark.asyncio
async def test_collector_invalid_payload_quarantined(
    migrated_db: dict[str, object],
) -> None:
    """Invalid payloads are logged and skipped, valid ones still written."""
    pool = migrated_db["pool"]
    valid = _load_fixtures()[0]
    invalid = {
        "stream": "ethusdt@aggTrade",
        "data": {"e": "aggTrade", "s": "ETHUSDT"},  # missing fields
    }
    ws = _FakeWs([invalid, valid])

    collector = BinanceAggTradeCollector(
        pool=pool,
        count_limit=50,
        time_limit_seconds=60.0,
    )
    with patch("aiohttp.ClientSession", return_value=_mock_session(ws)):
        task = asyncio.create_task(collector.start())
        await asyncio.wait_for(collector.wait_ready(), timeout=5.0)
        await asyncio.sleep(0.1)
        await collector.stop()
        await asyncio.wait_for(task, timeout=5.0)

    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT count(*) FROM bn_agg_trades")
    assert count == 1
