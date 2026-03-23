from __future__ import annotations

import asyncio
import logging
import threading

import pytest

from src.collectors.hyperliquid import HyperliquidWsManager
from src.collectors.hyperliquid.ws_close_support import close_manager_session

from ._ws_test_support import FakeHyperliquidSession, make_subscription


class _CountingSession(FakeHyperliquidSession):
    def __init__(self, on_open, on_close, on_error) -> None:
        super().__init__(on_open, on_close, on_error)
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1
        self.closed = True


class _SlowCloseSession(_CountingSession):
    def __init__(self, on_open, on_close, on_error) -> None:
        super().__init__(on_open, on_close, on_error)
        self.entered = threading.Event()
        self.release = threading.Event()

    def close(self) -> None:
        self.close_calls += 1
        self.entered.set()
        self.release.wait(timeout=1.0)
        self.closed = True


async def _build_manager() -> HyperliquidWsManager:
    async def handler(_message: object) -> None:
        return None

    return HyperliquidWsManager(
        base_url="https://api.hyperliquid.xyz",
        subscriptions=[make_subscription("trades", handler)],
    )


@pytest.mark.asyncio
async def test_close_manager_session_does_not_retry_completed_successful_close() -> (
    None
):
    manager = await _build_manager()
    session = _CountingSession(lambda: None, lambda _reason: None, lambda _error: None)
    session.started = True
    session.closed = True
    close_task = asyncio.create_task(asyncio.sleep(0))
    await close_task
    manager._session = session
    manager._failed_close_session = session
    manager._session_close_task = close_task
    manager._bridge.last_disconnect_reason = "network_drop"

    await close_manager_session(
        manager,
        reason="shutdown_cleanup",
        timeout_seconds=0.01,
        logger=logging.getLogger(__name__),
        retry_failed_session=True,
    )

    assert session.close_calls == 0
    assert manager._session is None
    assert manager._bridge.last_disconnect_reason == "network_drop"


@pytest.mark.asyncio
async def test_close_manager_session_preserves_disconnect_reason_on_timeout() -> None:
    manager = await _build_manager()
    session = _SlowCloseSession(lambda: None, lambda _reason: None, lambda _error: None)
    session.started = True
    manager._session = session
    manager._bridge.last_disconnect_reason = "network_drop"

    with pytest.raises(RuntimeError, match="timed out"):
        await close_manager_session(
            manager,
            reason="reconnect_cleanup",
            timeout_seconds=0.01,
            logger=logging.getLogger(__name__),
        )

    assert manager._bridge.last_disconnect_reason == "network_drop"
    assert manager._failed_close_session is session
    assert "TimeoutError" in manager._bridge.last_error

    session.release.set()
    await asyncio.wait_for(manager._session_close_task, timeout=1.0)
