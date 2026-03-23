from __future__ import annotations

import asyncio

import pytest

from src.collectors.hyperliquid import (
    HyperliquidMarketCollector,
    HyperliquidTradesCollector,
)

from .test_market_l2book import _load_fixture_messages
from ._ws_test_support import FakeHyperliquidSessionFactory


def _patch_stop_with_secondary_error(manager) -> None:
    original_stop = manager.stop

    async def stop_then_fail() -> None:
        await original_stop()
        raise RuntimeError("stop boom")

    manager.stop = stop_then_fail  # type: ignore[method-assign]


@pytest.mark.asyncio
async def test_market_collector_keeps_writer_failure_primary(
    migrated_db: dict[str, object],
) -> None:
    factory = FakeHyperliquidSessionFactory()
    collector = HyperliquidMarketCollector(
        base_url="https://api.hyperliquid.xyz",
        pool=migrated_db["pool"],
        count_limit=50,
        time_limit_seconds=60.0,
        enable_trades=False,
        enable_l2book=True,
        session_factory=factory,
    )

    async def writer_failed() -> None:
        raise RuntimeError("writer boom")

    assert collector._l2book_writer is not None
    collector._l2book_writer.wait_failure = writer_failed  # type: ignore[method-assign]
    _patch_stop_with_secondary_error(collector._manager)

    with pytest.raises(RuntimeError, match="writer boom") as raised:
        await asyncio.wait_for(asyncio.create_task(collector.start()), timeout=1.0)

    assert any("stop boom" in note for note in raised.value.__notes__ or [])


@pytest.mark.asyncio
async def test_market_collector_with_two_writers_flushes_pending_other_writer(
    migrated_db: dict[str, object],
) -> None:
    trigger = asyncio.Event()
    factory = FakeHyperliquidSessionFactory()
    collector = HyperliquidMarketCollector(
        base_url="https://api.hyperliquid.xyz",
        pool=migrated_db["pool"],
        count_limit=50,
        time_limit_seconds=60.0,
        enable_trades=True,
        enable_l2book=True,
        session_factory=factory,
    )

    async def writer_failed() -> None:
        await trigger.wait()
        raise RuntimeError("writer boom")

    assert collector._trade_writer is not None
    collector._trade_writer.wait_failure = writer_failed  # type: ignore[method-assign]
    _patch_stop_with_secondary_error(collector._manager)
    task = asyncio.create_task(collector.start())
    session = await factory.next_session()

    session.open()
    await collector.wait_ready()
    session.emit("l2Book:eth", _load_fixture_messages()[0])
    trigger.set()

    with pytest.raises(RuntimeError, match="writer boom") as raised:
        await asyncio.wait_for(task, timeout=1.0)

    assert any("stop boom" in note for note in raised.value.__notes__ or [])

    async with migrated_db["pool"].acquire() as connection:
        row_count = await connection.fetchval(
            "SELECT count(*) FROM hl_l2book WHERE source = 'hl_ws_l2book' AND coin = 'ETH'"
        )

    assert row_count == 1


@pytest.mark.asyncio
async def test_trades_collector_keeps_writer_failure_primary(
    migrated_db: dict[str, object],
) -> None:
    factory = FakeHyperliquidSessionFactory()
    collector = HyperliquidTradesCollector(
        base_url="https://api.hyperliquid.xyz",
        pool=migrated_db["pool"],
        count_limit=50,
        time_limit_seconds=60.0,
        session_factory=factory,
    )

    async def writer_failed() -> None:
        raise RuntimeError("writer boom")

    collector._writer.wait_failure = writer_failed  # type: ignore[method-assign]
    _patch_stop_with_secondary_error(collector._manager)

    with pytest.raises(RuntimeError, match="writer boom") as raised:
        await asyncio.wait_for(asyncio.create_task(collector.start()), timeout=1.0)

    assert any("stop boom" in note for note in raised.value.__notes__ or [])
