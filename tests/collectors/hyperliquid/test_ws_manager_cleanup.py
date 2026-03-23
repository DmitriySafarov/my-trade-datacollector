from __future__ import annotations

import asyncio
import logging

import pytest

from src.collectors.hyperliquid import HyperliquidWsManager

from ._ws_test_support import (
    FakeHyperliquidSession,
    FakeHyperliquidSessionFactory,
    make_subscription,
)


class _CloseTimeoutSession(FakeHyperliquidSession):
    def __init__(self, on_open, on_close, on_error, *, fail_on_close: bool) -> None:
        super().__init__(on_open, on_close, on_error)
        self.fail_on_close = fail_on_close
        self.close_called = False

    def close(self) -> None:
        self.close_called = True
        if self.fail_on_close:
            raise RuntimeError("Hyperliquid websocket thread did not stop")
        super().close()

    def disconnect(self, reason: str = "test_disconnect") -> None:
        self.on_close(reason)


@pytest.mark.asyncio
async def test_manager_surfaces_live_session_close_timeout(
    caplog: pytest.LogCaptureFixture,
) -> None:
    created = 0

    def build_session(on_open, on_close, on_error):
        nonlocal created
        created += 1
        return _CloseTimeoutSession(
            on_open,
            on_close,
            on_error,
            fail_on_close=created == 1,
        )

    factory = FakeHyperliquidSessionFactory(session_builder=build_session)

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
    caplog.set_level(logging.WARNING)
    task = asyncio.create_task(manager.run())
    first = await factory.next_session()

    first.open()
    await manager.wait_ready()
    first.disconnect("network_drop")
    await asyncio.sleep(0.05)

    with pytest.raises(RuntimeError, match="did not stop"):
        await asyncio.wait_for(task, timeout=1.0)

    assert len(factory.sessions) == 1
    assert first.close_called is True
    assert first.is_alive() is True
    assert "hyperliquid_ws_session_close_failed" in caplog.text
