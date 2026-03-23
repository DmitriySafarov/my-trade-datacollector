from __future__ import annotations

import asyncio

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


async def _wait_for(predicate, *, interval: float = 0.01) -> None:
    while not predicate():
        await asyncio.sleep(interval)
