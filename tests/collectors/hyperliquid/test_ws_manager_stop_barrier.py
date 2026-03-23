from __future__ import annotations

import asyncio
import threading

import pytest

from src.collectors.hyperliquid import HyperliquidWsManager, HyperliquidWsNotReadyError

from ._ws_test_support import (
    FakeHyperliquidSession,
    FakeHyperliquidSessionFactory,
    make_subscription,
)


class _DelayedCloseSession(FakeHyperliquidSession):
    def __init__(
        self,
        on_open,
        on_close,
        on_error,
        *,
        close_started: threading.Event,
        release_close: threading.Event,
    ) -> None:
        super().__init__(on_open, on_close, on_error)
        self._close_started = close_started
        self._release_close = release_close

    def close(self) -> None:
        self._close_started.set()
        self._release_close.wait(timeout=1.0)
        self.closed = True


class _ClosingCallbackSession(FakeHyperliquidSession):
    def close(self) -> None:
        super().close()
        self.on_close("closed_by_client")


@pytest.mark.asyncio
async def test_stop_drops_frames_emitted_while_close_is_blocked() -> None:
    close_started = threading.Event()
    release_close = threading.Event()
    factory = FakeHyperliquidSessionFactory(
        session_builder=lambda on_open, on_close, on_error: _DelayedCloseSession(
            on_open,
            on_close,
            on_error,
            close_started=close_started,
            release_close=release_close,
        )
    )
    handled: list[int] = []

    async def handler(message: object) -> None:
        handled.append(message["seq"])

    manager = HyperliquidWsManager(
        base_url="https://api.hyperliquid.xyz",
        subscriptions=[make_subscription("trades", handler)],
        session_factory=factory,
    )
    task = asyncio.create_task(manager.run())
    session = await factory.next_session()

    session.open()
    await manager.wait_ready()

    stop_task = asyncio.create_task(manager.stop())
    assert await asyncio.wait_for(asyncio.to_thread(close_started.wait, 1.0), 1.0)

    session.emit("trades:eth", {"seq": 1})
    release_close.set()

    await asyncio.wait_for(stop_task, timeout=1.0)
    await asyncio.wait_for(task, timeout=1.0)
    assert handled == []


@pytest.mark.asyncio
async def test_stop_unblocks_wait_ready_before_close_finishes() -> None:
    close_started = threading.Event()
    release_close = threading.Event()
    factory = FakeHyperliquidSessionFactory(
        session_builder=lambda on_open, on_close, on_error: _DelayedCloseSession(
            on_open,
            on_close,
            on_error,
            close_started=close_started,
            release_close=release_close,
        )
    )

    async def handler(_message: object) -> None:
        return None

    manager = HyperliquidWsManager(
        base_url="https://api.hyperliquid.xyz",
        subscriptions=[make_subscription("trades", handler)],
        session_factory=factory,
    )
    task = asyncio.create_task(manager.run())
    await factory.next_session()
    ready_task = asyncio.create_task(manager.wait_ready())
    stop_task = asyncio.create_task(manager.stop())

    assert await asyncio.wait_for(asyncio.to_thread(close_started.wait, 1.0), 1.0)
    with pytest.raises(
        HyperliquidWsNotReadyError,
        match="stopped before reaching ready state",
    ):
        await asyncio.wait_for(ready_task, timeout=1.0)

    release_close.set()
    await asyncio.wait_for(stop_task, timeout=1.0)
    await asyncio.wait_for(task, timeout=1.0)


@pytest.mark.asyncio
async def test_stop_close_callback_keeps_previous_disconnect_diagnosis() -> None:
    factory = FakeHyperliquidSessionFactory(session_builder=_ClosingCallbackSession)

    async def handler(_message: object) -> None:
        return None

    manager = HyperliquidWsManager(
        base_url="https://api.hyperliquid.xyz",
        subscriptions=[make_subscription("trades", handler)],
        session_factory=factory,
    )
    task = asyncio.create_task(manager.run())
    session = await factory.next_session()

    session.open()
    await manager.wait_ready()
    manager._bridge.last_disconnect_reason = "network_drop"

    await manager.stop()
    await asyncio.wait_for(task, timeout=1.0)

    snapshot = manager.health_snapshot()
    assert session.closed is True
    assert snapshot["state"] == "stopped"
    assert snapshot["last_disconnect_reason"] == "network_drop"
    assert snapshot["current_gap_started_at"] is None
    assert snapshot["last_gap_started_at"] is None
