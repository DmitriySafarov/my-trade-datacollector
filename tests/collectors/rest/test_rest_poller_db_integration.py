"""DB integration test: verify a REST poller can write to Bronze tables.

Uses a concrete poller that writes candle records to hl_rest_candles,
proving the full path from poll → parse → COPY → read-back.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import aiohttp
import asyncpg
import pytest

from src.collectors.rest.base_poller import BaseRestPoller


class _FakeCandleRestPoller(BaseRestPoller):
    """Writes a fixed candle row on each poll cycle (no real HTTP)."""

    def __init__(self, *, pool: asyncpg.Pool, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._pool = pool
        self.rows_written = 0

    async def _poll(self, session: aiohttp.ClientSession) -> None:
        now = datetime.now(timezone.utc)
        record = (
            now,  # time (= open_time per CHECK)
            "hl_rest_candles",  # source
            "ETH",  # coin
            "1h",  # interval
            now,  # open_time
            now,  # close_time
            3450.50,  # open
            3465.00,  # high
            3440.00,  # low
            3455.25,  # close
            12345.67,  # volume
            5000,  # trades_count
            "{}",  # payload
        )
        async with self._pool.acquire() as connection:
            await connection.copy_records_to_table(
                "hl_rest_candles",
                records=[record],
                columns=(
                    "time",
                    "source",
                    "coin",
                    "interval",
                    "open_time",
                    "close_time",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "trades_count",
                    "payload",
                ),
            )
        self.rows_written += 1


@pytest.mark.asyncio
async def test_rest_poller_writes_to_bronze_table(
    migrated_db: dict[str, object],
) -> None:
    pool: asyncpg.Pool = migrated_db["pool"]  # type: ignore[assignment]
    poller = _FakeCandleRestPoller(
        pool=pool,
        name="test_hl_rest_candles",
        source_ids=("hl_rest_candles",),
        interval_seconds=0.05,
        jitter_ratio=0.0,
        backoff_base_seconds=0.01,
    )

    task = asyncio.create_task(poller.start())
    await asyncio.wait_for(poller.wait_ready(), timeout=10.0)
    # Let it run a couple of polls.
    await asyncio.sleep(0.2)
    await poller.stop()
    await asyncio.wait_for(task, timeout=5.0)

    assert poller.rows_written >= 2

    async with pool.acquire() as connection:
        count = await connection.fetchval(
            "SELECT count(*) FROM hl_rest_candles WHERE source = 'hl_rest_candles'"
        )
    assert count == poller.rows_written


@pytest.mark.asyncio
async def test_rest_poller_health_after_db_write(
    migrated_db: dict[str, object],
) -> None:
    pool: asyncpg.Pool = migrated_db["pool"]  # type: ignore[assignment]
    poller = _FakeCandleRestPoller(
        pool=pool,
        name="test_hl_rest_candles",
        source_ids=("hl_rest_candles",),
        interval_seconds=0.05,
        jitter_ratio=0.0,
    )

    task = asyncio.create_task(poller.start())
    await asyncio.wait_for(poller.wait_ready(), timeout=10.0)
    snap = poller.health_snapshot()
    assert snap["total_polls"] >= 1
    assert snap["total_errors"] == 0
    assert snap["consecutive_failures"] == 0
    await poller.stop()
    await asyncio.wait_for(task, timeout=5.0)
