"""DB-backed integration tests for BinanceMarkPriceCollector."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import aiohttp
import pytest

from src.collectors.binance.mark_price_collector import BinanceMarkPriceCollector
from src.collectors.binance.mark_price_parsing import SOURCE_ID

FIXTURE_PATH = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "binance"
    / "ws_mark_price_messages.json"
)


def _load_fixtures() -> list[dict[str, Any]]:
    return json.loads(FIXTURE_PATH.read_text())


# ---------------------------------------------------------------------------
# WS mock helpers (same pattern as test_agg_trade_collector.py)
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
            n = await conn.fetchval("SELECT count(*) FROM bn_mark_price")
        if n >= count:
            return
        await asyncio.sleep(0.02)
    raise TimeoutError(f"Expected {count} rows, got {n}")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_collector_writes_mark_prices_on_stop(
    migrated_db: dict[str, object],
) -> None:
    """Messages received before stop() are flushed to bn_mark_price."""
    pool = migrated_db["pool"]
    fixtures = _load_fixtures()
    ws = _FakeWs(fixtures)

    collector = BinanceMarkPriceCollector(
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
        rows = await conn.fetch(
            """
            SELECT time, source, symbol, mark_price, index_price,
                   estimated_settle_price, premium, funding_rate,
                   next_funding_time, event_hash, payload, ingested_at
            FROM bn_mark_price
            ORDER BY symbol
            """
        )

    assert len(rows) == 2
    assert [r["symbol"] for r in rows] == ["BTCUSDT", "ETHUSDT"]
    assert [r["source"] for r in rows] == [SOURCE_ID, SOURCE_ID]
    assert [r["mark_price"] for r in rows] == [84200.10, 3021.50]
    assert [r["index_price"] for r in rows] == [84195.50, 3020.10]
    assert [r["estimated_settle_price"] for r in rows] == [84190.00, 3019.80]
    assert [r["funding_rate"] for r in rows] == [0.00015, 0.0001]
    for row in rows:
        assert row["time"].tzinfo is not None
        assert row["ingested_at"].tzinfo is not None
        assert row["event_hash"] is not None
        assert len(row["event_hash"]) == 64
        assert row["next_funding_time"] is not None
        premium = row["mark_price"] - row["index_price"]
        assert abs(row["premium"] - premium) < 1e-10
        payload = json.loads(row["payload"])
        assert payload["s"] == row["symbol"]


@pytest.mark.asyncio
async def test_collector_deduplicates_replayed_mark_prices(
    migrated_db: dict[str, object],
) -> None:
    """Replay guard trigger rejects duplicate event_hash for same source+time+symbol."""
    pool = migrated_db["pool"]
    fixture = _load_fixtures()[0]  # ETH message
    ws = _FakeWs([fixture, fixture])

    collector = BinanceMarkPriceCollector(
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
            SELECT count(*) FROM bn_mark_price
            WHERE source = $1 AND symbol = 'ETHUSDT'
            """,
            SOURCE_ID,
        )
    assert count == 1


@pytest.mark.asyncio
async def test_collector_flushes_on_timer(
    migrated_db: dict[str, object],
) -> None:
    """Rows are flushed by timer before stop()."""
    pool = migrated_db["pool"]
    fixtures = _load_fixtures()
    ws = _FakeWs(fixtures[:1])

    collector = BinanceMarkPriceCollector(
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

    collector = BinanceMarkPriceCollector(
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

    assert snap["name"] == "binance_mark_price"
    assert snap["source_ids"] == [SOURCE_ID]
    assert "writer" in snap
    assert snap["writer"]["name"] == "binance_mark_price_writer"


@pytest.mark.asyncio
async def test_collector_invalid_payload_quarantined(
    migrated_db: dict[str, object],
) -> None:
    """Invalid payloads are logged and skipped, valid ones still written."""
    pool = migrated_db["pool"]
    valid = _load_fixtures()[0]
    invalid = {
        "stream": "ethusdt@markPrice@1s",
        "data": {"e": "markPriceUpdate", "s": "ETHUSDT"},  # missing fields
    }
    ws = _FakeWs([invalid, valid])

    collector = BinanceMarkPriceCollector(
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
        count = await conn.fetchval("SELECT count(*) FROM bn_mark_price")
    assert count == 1
