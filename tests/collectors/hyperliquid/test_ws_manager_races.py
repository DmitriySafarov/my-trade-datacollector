from __future__ import annotations

import asyncio

import pytest

from src.collectors.hyperliquid import HyperliquidWsManager

from ._ws_test_support import FakeHyperliquidSessionFactory, make_subscription


@pytest.mark.asyncio
async def test_manager_preserves_received_messages_during_reconnect(
    monkeypatch,
) -> None:
    factory = FakeHyperliquidSessionFactory()
    received: list[object] = []
    delayed_calls: list[
        tuple[asyncio.AbstractEventLoop, object, tuple[object, ...]]
    ] = []

    def delayed_messages(loop, callback, *args) -> None:
        if getattr(callback, "__name__", "") == "_queue_message":
            delayed_calls.append((loop, callback, args))
            return
        if loop is not None and not loop.is_closed():
            loop.call_soon_threadsafe(callback, *args)

    async def handler(message: object) -> None:
        received.append(message)

    monkeypatch.setattr(
        "src.collectors.hyperliquid.ws_bridge.schedule_threadsafe",
        delayed_messages,
    )

    manager = HyperliquidWsManager(
        base_url="https://api.hyperliquid.xyz",
        subscriptions=[make_subscription("trades", handler)],
        reconnect_base_seconds=0.01,
        reconnect_max_seconds=0.01,
        reconnect_jitter_ratio=0.0,
        session_factory=factory,
    )
    task = asyncio.create_task(manager.run())
    first = await factory.next_session()

    first.open()
    await manager.wait_ready()
    first.emit("trades:eth", {"seq": 1})
    first.disconnect("network_drop")
    assert len(delayed_calls) == 1

    loop, callback, args = delayed_calls.pop()
    loop.call_soon(callback, *args)
    await asyncio.wait_for(_wait_for(lambda: received == [{"seq": 1}]), timeout=1.0)

    second = await asyncio.wait_for(factory.next_session(), timeout=1.0)
    second.open()
    await manager.wait_ready()
    assert manager.health_snapshot()["last_disconnect_reason"] == "network_drop"

    await manager.stop()
    await asyncio.wait_for(task, timeout=1.0)


@pytest.mark.asyncio
async def test_manager_clears_ready_after_silent_session_exit() -> None:
    factory = FakeHyperliquidSessionFactory()

    async def handler(_message: object) -> None:
        return None

    manager = HyperliquidWsManager(
        base_url="https://api.hyperliquid.xyz",
        subscriptions=[make_subscription("trades", handler)],
        reconnect_base_seconds=0.01,
        reconnect_max_seconds=0.01,
        reconnect_jitter_ratio=0.0,
        session_factory=factory,
    )
    task = asyncio.create_task(manager.run())
    first = await factory.next_session()

    first.open()
    await manager.wait_ready()
    first.closed = True

    await asyncio.wait_for(
        _wait_for(lambda: manager.health_snapshot()["ready"] is False),
        timeout=1.0,
    )
    assert manager.health_snapshot()["last_disconnect_reason"] == "session_exited"

    second = await asyncio.wait_for(factory.next_session(), timeout=1.0)
    second.open()
    await manager.wait_ready()
    assert manager.health_snapshot()["last_disconnect_reason"] == "session_exited"

    await manager.stop()
    await asyncio.wait_for(task, timeout=1.0)


async def _wait_for(predicate, *, interval: float = 0.01) -> None:
    while not predicate():
        await asyncio.sleep(interval)
