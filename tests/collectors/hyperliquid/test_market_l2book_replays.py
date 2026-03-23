from __future__ import annotations

import asyncio
from copy import deepcopy

import pytest

from ._ws_test_support import drain_session_callbacks
from .test_market_l2book import _load_fixture_messages, _start_l2book_collector


@pytest.mark.asyncio
async def test_l2book_collector_persists_shifted_time_replays(
    migrated_db: dict[str, object],
) -> None:
    pool = migrated_db["pool"]
    collector, task, session, _factory = await _start_l2book_collector(pool)
    message = _load_fixture_messages()[0]
    replay = deepcopy(message)
    replay["data"]["time"] += 10

    session.emit("l2Book:eth", message)
    session.emit("l2Book:eth", replay)
    await drain_session_callbacks()

    await collector.stop()
    await asyncio.wait_for(task, timeout=1.0)

    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT time, best_bid_price
            FROM hl_l2book
            WHERE source = 'hl_ws_l2book'
              AND coin = 'ETH'
            ORDER BY time
            """,
        )

    assert [row["best_bid_price"] for row in rows] == [3021.5, 3021.5]
    assert len(rows) == 2
