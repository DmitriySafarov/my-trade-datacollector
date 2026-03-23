from __future__ import annotations

import asyncio
import logging
from copy import deepcopy

import pytest

from ._ws_test_support import drain_session_callbacks
from .test_market_l2book import _load_fixture_messages, _start_l2book_collector


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mutator", "error_text"),
    [
        (
            lambda message: message["data"]["levels"][0][0].__setitem__("px", "NaN"),
            "must be finite",
        ),
        (
            lambda message: message["data"]["levels"][0][0].__setitem__("px", "1e309"),
            "finite float",
        ),
        (
            lambda message: (
                message["data"]["levels"][0][0].__setitem__("px", "1e308"),
                message["data"]["levels"][0][0].__setitem__("sz", "10"),
            ),
            "bid_notional",
        ),
        (
            lambda message: message["data"]["levels"][0][0].__setitem__(
                "px", "1e-5000"
            ),
            "finite float",
        ),
        (
            lambda message: message["data"].__setitem__("time", 253402300800000),
            "must be a valid UTC timestamp",
        ),
    ],
)
async def test_l2book_collector_skips_invalid_rows_and_keeps_valid_messages(
    migrated_db: dict[str, object],
    caplog: pytest.LogCaptureFixture,
    mutator,
    error_text: str,
) -> None:
    pool = migrated_db["pool"]
    collector, task, session, _factory = await _start_l2book_collector(pool)
    messages = _load_fixture_messages()
    invalid = deepcopy(messages[0])
    mutator(invalid)
    caplog.set_level(logging.WARNING)

    session.emit("l2Book:eth", invalid)
    session.emit("l2Book:btc", messages[1])
    await drain_session_callbacks()

    await collector.stop()
    await asyncio.wait_for(task, timeout=1.0)

    async with pool.acquire() as connection:
        rows = await connection.fetch(
            "SELECT coin, best_bid_price FROM hl_l2book ORDER BY coin"
        )

    assert [(row["coin"], row["best_bid_price"]) for row in rows] == [("BTC", 65000.5)]
    assert "hyperliquid_l2book_payload_invalid" in caplog.text
    assert error_text in caplog.text
