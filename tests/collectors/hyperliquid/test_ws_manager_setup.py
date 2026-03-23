from __future__ import annotations

import asyncio

import pytest

from src.collectors.hyperliquid import HyperliquidWsManager, HyperliquidWsNotReadyError

from ._ws_test_support import (
    FakeHyperliquidSession,
    FakeHyperliquidSessionFactory,
    make_subscription,
)


class _FailingStartSession(FakeHyperliquidSession):
    def start(self) -> None:
        super().start()
        raise RuntimeError("start boom")


@pytest.mark.asyncio
async def test_manager_reconnects_after_synchronous_start_failure() -> None:
    attempts = 0
    received: list[object] = []

    def build_session(on_open, on_close, on_error):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return _FailingStartSession(on_open, on_close, on_error)
        return FakeHyperliquidSession(on_open, on_close, on_error)

    factory = FakeHyperliquidSessionFactory(session_builder=build_session)

    async def handler(message: object) -> None:
        received.append(message)

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

    await asyncio.wait_for(_wait_for(lambda: first.closed), timeout=1.0)
    second = await asyncio.wait_for(factory.next_session(), timeout=1.0)
    assert manager.health_snapshot()["reconnect_count"] == 1
    assert task.done() is False

    second.open()
    await manager.wait_ready()
    second.emit("trades:eth", {"seq": 1})
    await asyncio.wait_for(_wait_for(lambda: received == [{"seq": 1}]), timeout=1.0)

    await manager.stop()
    await asyncio.wait_for(task, timeout=1.0)


@pytest.mark.asyncio
async def test_wait_ready_raises_after_terminal_pre_ready_stop() -> None:
    factory = FakeHyperliquidSessionFactory(session_builder=_FailingStartSession)

    async def handler(_message: object) -> None:
        return None

    manager = HyperliquidWsManager(
        base_url="https://api.hyperliquid.xyz",
        subscriptions=[make_subscription("trades", handler)],
        reconnect_base_seconds=30.0,
        reconnect_max_seconds=30.0,
        reconnect_jitter_ratio=0.0,
        session_factory=factory,
    )
    task = asyncio.create_task(manager.run())
    ready_task = asyncio.create_task(manager.wait_ready())
    first = await factory.next_session()

    await asyncio.wait_for(_wait_for(lambda: first.closed), timeout=1.0)
    await manager.stop()

    with pytest.raises(
        HyperliquidWsNotReadyError,
        match="stopped before reaching ready state",
    ):
        await asyncio.wait_for(ready_task, timeout=1.0)
    await asyncio.wait_for(task, timeout=1.0)


@pytest.mark.asyncio
async def test_manager_reconnects_after_open_timeout() -> None:
    factory = FakeHyperliquidSessionFactory()
    received: list[object] = []

    async def handler(message: object) -> None:
        received.append(message)

    manager = HyperliquidWsManager(
        base_url="https://api.hyperliquid.xyz",
        subscriptions=[make_subscription("trades", handler)],
        reconnect_base_seconds=0.01,
        reconnect_max_seconds=0.01,
        reconnect_jitter_ratio=0.0,
        open_timeout_seconds=0.05,
        session_factory=factory,
    )
    task = asyncio.create_task(manager.run())
    first = await factory.next_session()

    await asyncio.wait_for(_wait_for(lambda: first.closed), timeout=1.0)
    second = await asyncio.wait_for(factory.next_session(), timeout=1.0)
    assert manager.health_snapshot()["last_disconnect_reason"] == "open_timeout"
    assert manager.health_snapshot()["reconnect_count"] == 1

    second.open()
    await manager.wait_ready()
    second.emit("trades:eth", {"seq": 2})
    await asyncio.wait_for(_wait_for(lambda: received == [{"seq": 2}]), timeout=1.0)

    await manager.stop()
    await asyncio.wait_for(task, timeout=1.0)


@pytest.mark.asyncio
async def test_late_open_after_timeout_does_not_restore_ready(monkeypatch) -> None:
    factory = FakeHyperliquidSessionFactory()
    release_backoff = asyncio.Event()

    async def handler(_message: object) -> None:
        return None

    async def blocked_backoff(stop_event: asyncio.Event, delay: float) -> bool:
        del stop_event, delay
        await release_backoff.wait()
        return False

    monkeypatch.setattr(
        "src.collectors.hyperliquid.ws_manager.wait_for_stop_or_timeout",
        blocked_backoff,
    )

    manager = HyperliquidWsManager(
        base_url="https://api.hyperliquid.xyz",
        subscriptions=[make_subscription("trades", handler)],
        reconnect_base_seconds=0.01,
        reconnect_max_seconds=0.01,
        reconnect_jitter_ratio=0.0,
        open_timeout_seconds=0.05,
        session_factory=factory,
    )
    task = asyncio.create_task(manager.run())
    first = await factory.next_session()

    await asyncio.wait_for(
        _wait_for(
            lambda: (
                manager.health_snapshot()["last_disconnect_reason"] == "open_timeout"
            )
        ),
        timeout=1.0,
    )
    first.open()
    await asyncio.sleep(0)
    assert manager.health_snapshot()["ready"] is False
    assert manager.health_snapshot()["state"] == "reconnecting"

    release_backoff.set()
    second = await asyncio.wait_for(factory.next_session(), timeout=1.0)
    second.open()
    await manager.wait_ready()

    await manager.stop()
    await asyncio.wait_for(task, timeout=1.0)


async def _wait_for(predicate, *, interval: float = 0.01) -> None:
    while not predicate():
        await asyncio.sleep(interval)
