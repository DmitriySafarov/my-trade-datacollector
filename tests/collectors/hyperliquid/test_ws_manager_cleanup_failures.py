from __future__ import annotations

import asyncio
import threading

import pytest

from src.collectors.hyperliquid import HyperliquidWsManager

from ._ws_test_support import (
    FakeHyperliquidSession,
    FakeHyperliquidSessionFactory,
    make_subscription,
)


class _AlwaysFailCloseSession(FakeHyperliquidSession):
    def __init__(self, on_open, on_close, on_error, *, attempts: list[int]) -> None:
        super().__init__(on_open, on_close, on_error)
        self._attempts = attempts

    def close(self) -> None:
        self._attempts[0] += 1
        raise RuntimeError("Hyperliquid websocket session close timed out")

    def disconnect(self, reason: str = "test_disconnect") -> None:
        self.on_close(reason)


@pytest.mark.asyncio
async def test_manager_retries_failed_close_once_in_final_cleanup(
    caplog: pytest.LogCaptureFixture,
) -> None:
    attempts = [0]

    async def handler(_message: object) -> None:
        return None

    factory = FakeHyperliquidSessionFactory(
        session_builder=lambda on_open, on_close, on_error: _AlwaysFailCloseSession(
            on_open,
            on_close,
            on_error,
            attempts=attempts,
        )
    )
    manager = HyperliquidWsManager(
        base_url="https://api.hyperliquid.xyz",
        subscriptions=[make_subscription("trades", handler)],
        reconnect_base_seconds=0.01,
        reconnect_max_seconds=0.01,
        reconnect_jitter_ratio=0.0,
        session_factory=factory,
    )
    caplog.set_level("WARNING")
    task = asyncio.create_task(manager.run())
    session = await factory.next_session()

    session.open()
    await manager.wait_ready()
    session.disconnect("network_drop")

    with pytest.raises(RuntimeError, match="timed out"):
        await asyncio.wait_for(task, timeout=1.0)

    assert attempts == [2]
    assert manager._session is session
    assert caplog.text.count("hyperliquid_ws_session_close_failed") == 2


@pytest.mark.asyncio
async def test_manager_drains_accepted_messages_before_stop_when_close_fails() -> None:
    factory = FakeHyperliquidSessionFactory(
        session_builder=lambda on_open, on_close, on_error: _AlwaysFailCloseSession(
            on_open,
            on_close,
            on_error,
            attempts=[0],
        )
    )
    release = asyncio.Event()
    handled: list[int] = []

    async def handler(message: object) -> None:
        handled.append(message["seq"])
        if message["seq"] == 1:
            await release.wait()

    manager = HyperliquidWsManager(
        base_url="https://api.hyperliquid.xyz",
        subscriptions=[make_subscription("trades", handler)],
        reconnect_base_seconds=0.01,
        reconnect_max_seconds=0.01,
        reconnect_jitter_ratio=0.0,
        shutdown_drain_timeout_seconds=1.0,
        session_factory=factory,
    )
    task = asyncio.create_task(manager.run())
    session = await factory.next_session()

    session.open()
    await manager.wait_ready()
    session.emit("trades:eth", {"seq": 1})
    session.emit("trades:eth", {"seq": 2})
    await asyncio.wait_for(_wait_for(lambda: handled == [1]), timeout=1.0)

    session.disconnect("network_drop")
    release.set()

    with pytest.raises(RuntimeError, match="timed out"):
        await asyncio.wait_for(task, timeout=1.0)

    assert handled == [1, 2]


@pytest.mark.asyncio
async def test_manager_reuses_inflight_close_task_on_final_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = threading.Event()
    release = threading.Event()
    attempts = 0
    max_active = 0
    active = 0

    class SlowCloseSession(FakeHyperliquidSession):
        def close(self) -> None:
            nonlocal attempts, active, max_active
            attempts += 1
            active += 1
            max_active = max(max_active, active)
            entered.set()
            release.wait(timeout=0.5)
            self.closed = True
            active -= 1

        def disconnect(self, reason: str = "test_disconnect") -> None:
            self.on_close(reason)

    async def handler(_message: object) -> None:
        return None

    monkeypatch.setattr(
        "src.collectors.hyperliquid.ws_manager.SESSION_CLOSE_TIMEOUT_SECONDS",
        0.01,
    )
    factory = FakeHyperliquidSessionFactory(session_builder=SlowCloseSession)
    manager = HyperliquidWsManager(
        base_url="https://api.hyperliquid.xyz",
        subscriptions=[make_subscription("trades", handler)],
        reconnect_base_seconds=0.01,
        reconnect_max_seconds=0.01,
        reconnect_jitter_ratio=0.0,
        session_factory=factory,
    )
    task = asyncio.create_task(manager.run())
    session = await factory.next_session()

    session.open()
    await manager.wait_ready()
    session.disconnect("network_drop")
    await asyncio.wait_for(asyncio.to_thread(entered.wait, 1.0), timeout=2.0)

    with pytest.raises(RuntimeError, match="timed out"):
        await asyncio.wait_for(task, timeout=1.0)

    release.set()
    await asyncio.sleep(0.05)
    assert attempts == 1
    assert max_active == 1


async def _wait_for(predicate, *, interval: float = 0.01) -> None:
    while not predicate():
        await asyncio.sleep(interval)
