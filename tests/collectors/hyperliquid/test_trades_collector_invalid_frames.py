from __future__ import annotations

import asyncio
import logging

import pytest

from ._ws_test_support import drain_session_callbacks
from .test_trades_collector import _load_fixture_messages, _start_collector


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "message",
    [
        ["not", "a", "mapping"],
        {"channel": "l2Book", "data": []},
        {"channel": "trades", "data": {"coin": "ETH"}},
    ],
)
async def test_trades_collector_skips_invalid_envelopes(
    migrated_db: dict[str, object],
    caplog: pytest.LogCaptureFixture,
    message: object,
) -> None:
    pool = migrated_db["pool"]
    collector, task, session, _factory = await _start_collector(pool)
    caplog.set_level(logging.WARNING)

    session.emit("trades:eth", message)
    session.emit("trades:eth", _load_fixture_messages()[0])
    await drain_session_callbacks()

    await collector.stop()
    await asyncio.wait_for(task, timeout=1.0)

    async with pool.acquire() as connection:
        row_count = await connection.fetchval("SELECT count(*) FROM hl_trades")

    assert row_count == 1
    assert "hyperliquid_trade_message_invalid" in caplog.text
