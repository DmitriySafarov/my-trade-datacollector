from __future__ import annotations

import asyncio
import json
from copy import deepcopy

import pytest

from ._ws_test_support import drain_session_callbacks
from .test_market_l2book import _load_fixture_messages, _start_l2book_collector


@pytest.mark.asyncio
async def test_l2book_collector_preserves_raw_payload_while_normalizing_typed_levels(
    migrated_db: dict[str, object],
) -> None:
    pool = migrated_db["pool"]
    collector, task, session, _factory = await _start_l2book_collector(pool)
    message = deepcopy(_load_fixture_messages()[0])
    message["data"]["levels"][0][0]["sz"] = "8.2000"
    message["data"]["extra"] = {"field": "raw"}

    session.emit("l2Book:eth", message)
    await drain_session_callbacks()
    await collector.stop()
    await asyncio.wait_for(task, timeout=1.0)

    async with pool.acquire() as connection:
        row = await connection.fetchrow(
            "SELECT bids, payload FROM hl_l2book WHERE coin = 'ETH'"
        )

    assert json.loads(row["payload"])["extra"] == {"field": "raw"}
    assert json.loads(row["payload"])["levels"][0][0]["sz"] == "8.2000"
    assert json.loads(row["bids"])[0]["sz"] == "8.2"
