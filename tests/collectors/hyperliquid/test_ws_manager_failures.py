from __future__ import annotations

import asyncio

import pytest

from src.collectors.hyperliquid import HyperliquidWsManager

from ._ws_test_support import (
    FakeHyperliquidSession,
    FakeHyperliquidSessionFactory,
    make_subscription,
)


class _ClosingCallbackSession(FakeHyperliquidSession):
    def close(self) -> None:
        super().close()
        self.on_close("closed_by_client")


@pytest.mark.asyncio
async def test_manager_stop_interrupts_reconnect_backoff() -> None:
    factory = FakeHyperliquidSessionFactory()

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
    first = await factory.next_session()

    first.open()
    await manager.wait_ready()
    first.disconnect("network_drop")
    await asyncio.wait_for(
        _wait_for(lambda: manager.health_snapshot()["reconnect_count"] == 1),
        timeout=1.0,
    )

    await manager.stop()
    await asyncio.wait_for(task, timeout=1.0)
    assert len(factory.sessions) == 1


@pytest.mark.asyncio
async def test_manager_resets_backoff_after_successful_open(monkeypatch) -> None:
    factory = FakeHyperliquidSessionFactory()
    delays: list[float] = []

    async def handler(_message: object) -> None:
        return None

    async def record_backoff(stop_event: asyncio.Event, delay: float) -> bool:
        del stop_event
        delays.append(delay)
        return False

    monkeypatch.setattr(
        "src.collectors.hyperliquid.ws_manager.wait_for_stop_or_timeout",
        record_backoff,
    )

    manager = HyperliquidWsManager(
        base_url="https://api.hyperliquid.xyz",
        subscriptions=[make_subscription("trades", handler)],
        reconnect_base_seconds=1.0,
        reconnect_max_seconds=30.0,
        reconnect_jitter_ratio=0.0,
        session_factory=factory,
    )
    task = asyncio.create_task(manager.run())
    first = await factory.next_session()

    first.disconnect("failed_before_open")
    second = await factory.next_session()
    second.disconnect("failed_before_open")
    third = await factory.next_session()
    third.open()
    await manager.wait_ready()
    third.disconnect("dropped_after_open")
    await factory.next_session()

    assert delays[:3] == [1.0, 2.0, 1.0]

    await manager.stop()
    await asyncio.wait_for(task, timeout=1.0)


@pytest.mark.asyncio
async def test_manager_error_callbacks_trigger_reconnect() -> None:
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
    first.fail(RuntimeError("socket boom"))

    second = await asyncio.wait_for(factory.next_session(), timeout=1.0)
    assert manager.health_snapshot()["last_disconnect_reason"] == "websocket_error"
    second.open()
    await asyncio.wait_for(
        _wait_for(lambda: manager.health_snapshot()["reconnect_count"] == 1),
        timeout=1.0,
    )
    assert first.closed is True
    assert manager.health_snapshot()["last_disconnect_reason"] == "websocket_error"
    assert manager.health_snapshot()["last_error"] == "RuntimeError('socket boom')"

    await manager.stop()
    await asyncio.wait_for(task, timeout=1.0)


@pytest.mark.asyncio
async def test_manager_error_reason_survives_cleanup_close_callback() -> None:
    factory = FakeHyperliquidSessionFactory(session_builder=_ClosingCallbackSession)

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
    first.fail(RuntimeError("socket boom"))

    second = await asyncio.wait_for(factory.next_session(), timeout=1.0)
    assert manager.health_snapshot()["last_disconnect_reason"] == "websocket_error"
    second.open()
    await manager.wait_ready()

    await manager.stop()
    await asyncio.wait_for(task, timeout=1.0)


@pytest.mark.asyncio
async def test_manager_surfaces_handler_failures() -> None:
    factory = FakeHyperliquidSessionFactory()

    async def handler(_message: object) -> None:
        raise RuntimeError("boom")

    manager = HyperliquidWsManager(
        base_url="https://api.hyperliquid.xyz",
        subscriptions=[make_subscription("trades", handler)],
        session_factory=factory,
    )
    task = asyncio.create_task(manager.run())
    session = await factory.next_session()

    session.open()
    await manager.wait_ready()
    session.emit("trades:eth", {"channel": "trades", "data": [{"coin": "ETH"}]})

    with pytest.raises(RuntimeError, match="boom"):
        await asyncio.wait_for(task, timeout=1.0)
    assert manager.health_snapshot()["last_error"] == "RuntimeError('boom')"


async def _wait_for(predicate, *, interval: float = 0.01) -> None:
    while not predicate():
        await asyncio.sleep(interval)
