from __future__ import annotations

import asyncio
import logging

import pytest

from src.collectors.hyperliquid import HyperliquidWsManager

from ._ws_test_support import FakeHyperliquidSessionFactory, make_subscription


@pytest.mark.asyncio
async def test_stop_waits_for_bounded_queue_drain() -> None:
    factory = FakeHyperliquidSessionFactory()
    handled: list[int] = []

    async def handler(message: object) -> None:
        handled.append(message["seq"])
        await asyncio.sleep(0.05)

    manager = HyperliquidWsManager(
        base_url="https://api.hyperliquid.xyz",
        subscriptions=[make_subscription("trades", handler)],
        reconnect_jitter_ratio=0.0,
        shutdown_drain_timeout_seconds=1.0,
        session_factory=factory,
    )
    task = asyncio.create_task(manager.run())
    session = await factory.next_session()

    session.open()
    await manager.wait_ready()
    for seq in (1, 2, 3):
        session.emit("trades:eth", {"seq": seq})
    await asyncio.wait_for(_wait_for(lambda: handled == [1]), timeout=1.0)

    await manager.stop()

    assert handled == [1, 2, 3]
    assert task.done() is True


@pytest.mark.asyncio
async def test_stop_surfaces_drain_timeout_as_shutdown_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    factory = FakeHyperliquidSessionFactory()
    handled: list[int] = []
    release = asyncio.Event()

    async def handler(message: object) -> None:
        handled.append(message["seq"])
        await release.wait()

    manager = HyperliquidWsManager(
        base_url="https://api.hyperliquid.xyz",
        subscriptions=[make_subscription("trades", handler)],
        reconnect_jitter_ratio=0.0,
        shutdown_drain_timeout_seconds=0.01,
        session_factory=factory,
    )
    caplog.set_level(logging.WARNING)
    task = asyncio.create_task(manager.run())
    session = await factory.next_session()

    session.open()
    await manager.wait_ready()
    for seq in (1, 2, 3):
        session.emit("trades:eth", {"seq": seq})
    await asyncio.wait_for(_wait_for(lambda: handled == [1]), timeout=1.0)

    await manager.stop()

    assert handled == [1]
    assert "hyperliquid_ws_shutdown_drain_timeout" in caplog.text
    with pytest.raises(RuntimeError, match="shutdown drain timed out"):
        await asyncio.wait_for(task, timeout=1.0)


@pytest.mark.asyncio
async def test_stop_prefers_worker_failure_over_shutdown_drain_timeout(
    caplog: pytest.LogCaptureFixture,
) -> None:
    factory = FakeHyperliquidSessionFactory()
    started = asyncio.Event()
    release = asyncio.Event()

    async def handler(message: object) -> None:
        if message["seq"] == 1:
            started.set()
            await release.wait()
        raise RuntimeError("boom")

    manager = HyperliquidWsManager(
        base_url="https://api.hyperliquid.xyz",
        subscriptions=[make_subscription("trades", handler)],
        reconnect_jitter_ratio=0.0,
        shutdown_drain_timeout_seconds=1.0,
        session_factory=factory,
    )
    caplog.set_level(logging.WARNING)
    task = asyncio.create_task(manager.run())
    session = await factory.next_session()

    session.open()
    await manager.wait_ready()
    session.emit("trades:eth", {"seq": 1})
    session.emit("trades:eth", {"seq": 2})
    await asyncio.wait_for(started.wait(), timeout=1.0)

    stop_task = asyncio.create_task(manager.stop())
    release.set()

    await asyncio.wait_for(stop_task, timeout=1.0)
    assert "hyperliquid_ws_shutdown_drain_timeout" not in caplog.text
    with pytest.raises(RuntimeError, match="boom"):
        await asyncio.wait_for(task, timeout=1.0)


async def _wait_for(predicate, *, interval: float = 0.01) -> None:
    while not predicate():
        await asyncio.sleep(interval)
