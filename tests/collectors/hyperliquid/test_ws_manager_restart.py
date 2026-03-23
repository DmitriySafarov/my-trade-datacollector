from __future__ import annotations

import asyncio

import pytest

from src.collectors.hyperliquid import HyperliquidWsManager

from ._ws_test_support import FakeHyperliquidSessionFactory, make_subscription


@pytest.mark.asyncio
async def test_manager_can_run_again_after_stop() -> None:
    factory = FakeHyperliquidSessionFactory()
    received: list[object] = []

    async def handler(message: object) -> None:
        received.append(message)

    manager = HyperliquidWsManager(
        base_url="https://api.hyperliquid.xyz",
        subscriptions=[make_subscription("trades", handler)],
        session_factory=factory,
    )
    for expected_count in (1, 2):
        task = asyncio.create_task(manager.run())
        session = await asyncio.wait_for(factory.next_session(), timeout=1.0)
        session.open()
        await manager.wait_ready()
        session.emit("trades:eth", {"seq": expected_count})
        await asyncio.wait_for(_wait_for_count(received, expected_count), timeout=1.0)
        await manager.stop()
        await asyncio.wait_for(task, timeout=1.0)

    assert received == [{"seq": 1}, {"seq": 2}]


@pytest.mark.asyncio
async def test_manager_stop_before_first_run_body_exits_scheduled_run() -> None:
    factory = FakeHyperliquidSessionFactory()

    async def handler(_message: object) -> None:
        return None

    manager = HyperliquidWsManager(
        base_url="https://api.hyperliquid.xyz",
        subscriptions=[make_subscription("trades", handler)],
        session_factory=factory,
    )

    task = asyncio.create_task(manager.run())
    await manager.stop()
    await asyncio.wait_for(task, timeout=1.0)

    assert factory.sessions == []


@pytest.mark.asyncio
async def test_manager_can_run_again_after_handler_failure() -> None:
    factory = FakeHyperliquidSessionFactory()
    received: list[object] = []
    fail_first = True

    async def handler(message: object) -> None:
        nonlocal fail_first
        if fail_first:
            fail_first = False
            raise RuntimeError("boom")
        received.append(message)

    manager = HyperliquidWsManager(
        base_url="https://api.hyperliquid.xyz",
        subscriptions=[make_subscription("trades", handler)],
        session_factory=factory,
    )

    first_task = asyncio.create_task(manager.run())
    first_session = await asyncio.wait_for(factory.next_session(), timeout=1.0)
    first_session.open()
    await manager.wait_ready()
    first_session.emit("trades:eth", {"seq": 1})

    with pytest.raises(RuntimeError, match="boom"):
        await asyncio.wait_for(first_task, timeout=1.0)

    second_task = asyncio.create_task(manager.run())
    second_session = await asyncio.wait_for(factory.next_session(), timeout=1.0)
    second_session.open()
    await manager.wait_ready()
    second_session.emit("trades:eth", {"seq": 2})
    await asyncio.wait_for(_wait_for_count(received, 1), timeout=1.0)

    await manager.stop()
    await asyncio.wait_for(second_task, timeout=1.0)

    assert received == [{"seq": 2}]


@pytest.mark.asyncio
async def test_manager_restart_drops_stale_queued_messages_after_fatal_failure() -> (
    None
):
    factory = FakeHyperliquidSessionFactory()
    received: list[object] = []
    first_failure = True

    async def handler(message: object) -> None:
        nonlocal first_failure
        if first_failure:
            first_failure = False
            raise RuntimeError("boom")
        received.append(message)

    manager = HyperliquidWsManager(
        base_url="https://api.hyperliquid.xyz",
        subscriptions=[make_subscription("trades", handler)],
        queue_maxsize=4,
        session_factory=factory,
    )

    first_task = asyncio.create_task(manager.run())
    first = await asyncio.wait_for(factory.next_session(), timeout=1.0)
    first.open()
    await manager.wait_ready()
    first.emit("trades:eth", {"seq": 1})
    first.emit("trades:eth", {"seq": 99})

    with pytest.raises(RuntimeError, match="boom"):
        await asyncio.wait_for(first_task, timeout=1.0)

    second_task = asyncio.create_task(manager.run())
    second = await asyncio.wait_for(factory.next_session(), timeout=1.0)
    second.open()
    await manager.wait_ready()
    await asyncio.sleep(0.05)

    assert received == []

    second.emit("trades:eth", {"seq": 2})
    await asyncio.wait_for(_wait_for_count(received, 1), timeout=1.0)
    await manager.stop()
    await asyncio.wait_for(second_task, timeout=1.0)

    assert received == [{"seq": 2}]


async def _wait_for_count(received: list[object], expected_count: int) -> None:
    while len(received) < expected_count:
        await asyncio.sleep(0.01)
