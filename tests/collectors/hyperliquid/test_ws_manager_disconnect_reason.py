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
async def test_cleanup_close_preserves_websocket_error_reason() -> None:
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
    first = await asyncio.wait_for(factory.next_session(), timeout=1.0)

    first.open()
    await manager.wait_ready()
    first.fail(RuntimeError("socket boom"))

    await asyncio.wait_for(factory.next_session(), timeout=1.0)

    assert manager.health_snapshot()["last_disconnect_reason"] == "websocket_error"
    assert manager.health_snapshot()["last_error"] == "RuntimeError('socket boom')"

    await manager.stop()
    await asyncio.wait_for(task, timeout=1.0)
