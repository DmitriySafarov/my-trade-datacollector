from __future__ import annotations

import pytest

from src.collectors.hyperliquid.ws_bridge import HyperliquidWsBridge
from src.collectors.hyperliquid.ws_support import schedule_threadsafe

from ._ws_test_support import make_subscription


def test_schedule_threadsafe_returns_false_when_call_races_loop_close() -> None:
    class ClosingLoop:
        def is_closed(self) -> bool:
            return False

        def call_soon_threadsafe(self, *_args: object) -> None:
            raise RuntimeError("Event loop is closed")

    assert schedule_threadsafe(ClosingLoop(), lambda: None) is False


@pytest.mark.asyncio
async def test_dropped_enqueue_schedule_releases_reserved_slot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def handler(_message: object) -> None:
        return None

    bridge = HyperliquidWsBridge(
        subscriptions=[make_subscription("trades", handler)],
        queue_maxsize=1,
        on_fatal=lambda: None,
    )
    await bridge.start()
    monkeypatch.setattr(
        "src.collectors.hyperliquid.ws_bridge.schedule_threadsafe",
        lambda _loop, _callback, *args: False,
    )
    try:
        bridge.message_callback(0, "trades:eth")({"seq": 1})

        assert bridge._message_slots.is_empty() is True
        assert await bridge.drain(timeout=0.1) is True
    finally:
        await bridge.stop()
