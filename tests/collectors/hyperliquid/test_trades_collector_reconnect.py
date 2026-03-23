from __future__ import annotations

import asyncio
from copy import deepcopy

import pytest

from ._ws_test_support import drain_session_callbacks
from .test_trades_collector import _load_fixture_messages, _start_collector


@pytest.mark.asyncio
async def test_trades_collector_persists_across_reconnect_without_duplicate_tids(
    migrated_db: dict[str, object],
) -> None:
    pool = migrated_db["pool"]
    collector, task, session, factory = await _start_collector(
        pool,
        reconnect_base_seconds=0.01,
        reconnect_max_seconds=0.01,
        reconnect_jitter_ratio=0.0,
    )
    message = _load_fixture_messages()[0]
    replay = deepcopy(message)
    resumed = deepcopy(message)
    resumed["data"][0]["tid"] = 91003
    resumed["data"][0]["hash"] = "0xethtrade003"
    resumed["data"][0]["time"] += 10

    session.emit("trades:eth", message)
    session.disconnect("network_drop")

    replacement = await asyncio.wait_for(factory.next_session(), timeout=1.0)
    replacement.open()
    await collector.wait_ready()
    replacement.emit("trades:eth", replay)
    replacement.emit("trades:eth", resumed)
    await drain_session_callbacks()

    await collector.stop()
    await asyncio.wait_for(task, timeout=1.0)

    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT tid
            FROM hl_trades
            WHERE source = 'hl_ws_trades'
              AND coin = 'ETH'
            ORDER BY tid
            """,
        )

    assert [row["tid"] for row in rows] == [91001, 91003]
