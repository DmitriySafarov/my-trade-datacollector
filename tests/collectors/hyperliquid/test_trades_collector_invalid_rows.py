from __future__ import annotations

import asyncio
import logging
from copy import deepcopy

import pytest

from ._ws_test_support import drain_session_callbacks
from .test_trades_collector import _load_fixture_messages, _start_collector


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value", "error_text"),
    [
        ("tid", "91001.5", "must be an integer"),
        ("tid", 9_223_372_036_854_775_808, "must fit PostgreSQL BIGINT"),
        ("time", 253402300800000, "must be a valid UTC timestamp"),
    ],
)
async def test_trades_collector_skips_invalid_rows_and_keeps_valid_siblings(
    migrated_db: dict[str, object],
    caplog: pytest.LogCaptureFixture,
    field: str,
    value: object,
    error_text: str,
) -> None:
    pool = migrated_db["pool"]
    collector, task, session, _factory = await _start_collector(pool)
    messages = _load_fixture_messages()
    mixed_message = deepcopy(messages[0])
    invalid_trade = deepcopy(mixed_message["data"][0])
    invalid_trade[field] = value
    mixed_message["data"].append(invalid_trade)
    caplog.set_level(logging.WARNING)

    session.emit("trades:eth", mixed_message)
    session.emit("trades:btc", messages[1])
    await drain_session_callbacks()

    await collector.stop()
    await asyncio.wait_for(task, timeout=1.0)

    async with pool.acquire() as connection:
        rows = await connection.fetch(
            "SELECT coin, tid FROM hl_trades ORDER BY coin, tid"
        )

    assert [(row["coin"], row["tid"]) for row in rows] == [
        ("BTC", 91002),
        ("ETH", 91001),
    ]
    assert "hyperliquid_trade_payload_invalid" in caplog.text
    assert error_text in caplog.text
