from __future__ import annotations

import asyncio
import logging
from copy import deepcopy

import pytest

from ._ws_test_support import drain_session_callbacks
from .test_market_l2book import _load_fixture_messages, _start_l2book_collector


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "message",
    [
        ["not", "a", "mapping"],
        {"channel": "trades", "data": []},
        {"channel": "l2Book", "data": ["not", "a", "mapping"]},
    ],
)
async def test_l2book_collector_skips_invalid_envelopes(
    migrated_db: dict[str, object],
    caplog: pytest.LogCaptureFixture,
    message: object,
) -> None:
    pool = migrated_db["pool"]
    collector, task, session, _factory = await _start_l2book_collector(pool)
    caplog.set_level(logging.WARNING)

    session.emit("l2Book:eth", message)
    session.emit("l2Book:eth", _load_fixture_messages()[0])
    await drain_session_callbacks()

    await collector.stop()
    await asyncio.wait_for(task, timeout=1.0)

    async with pool.acquire() as connection:
        row_count = await connection.fetchval("SELECT count(*) FROM hl_l2book")

    assert row_count == 1
    assert "hyperliquid_l2book_payload_invalid" in caplog.text


@pytest.mark.asyncio
async def test_l2book_collector_rejects_messages_routed_to_the_wrong_coin(
    migrated_db: dict[str, object],
    caplog: pytest.LogCaptureFixture,
) -> None:
    pool = migrated_db["pool"]
    collector, task, session, _factory = await _start_l2book_collector(pool)
    caplog.set_level(logging.WARNING)
    wrong_coin = deepcopy(_load_fixture_messages()[1])

    session.emit("l2Book:eth", wrong_coin)
    await drain_session_callbacks()

    await collector.stop()
    await asyncio.wait_for(task, timeout=1.0)

    async with pool.acquire() as connection:
        row_count = await connection.fetchval("SELECT count(*) FROM hl_l2book")

    assert row_count == 0
    assert "Unexpected Hyperliquid l2Book coin: BTC" in caplog.text
