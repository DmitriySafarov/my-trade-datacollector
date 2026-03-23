from __future__ import annotations

import asyncio

import pytest

from src.collectors.hyperliquid.ws_bridge import HyperliquidWsBridge

from ._ws_test_support import make_subscription


@pytest.mark.asyncio
async def test_bridge_rejects_stale_queue_messages_after_restart() -> None:
    handled: list[object] = []

    async def handler(message: object) -> None:
        handled.append(message)

    bridge = HyperliquidWsBridge(
        subscriptions=[make_subscription("trades", handler)],
        queue_maxsize=10,
        on_fatal=lambda: None,
    )
    await bridge.start()
    try:
        bridge.handle_open(0, asyncio.Event())
        old_run_token = bridge._run_token
    finally:
        await bridge.stop()

    await bridge.start()
    try:
        bridge.handle_open(0, asyncio.Event())
        bridge._queue_message(0, old_run_token, "trades:eth", {"seq": 1})
        bridge.message_callback(0, "trades:eth")({"seq": 2})

        await asyncio.wait_for(_wait_for(lambda: handled == [{"seq": 2}]), timeout=1.0)
        assert handled == [{"seq": 2}]
    finally:
        await bridge.stop()


@pytest.mark.asyncio
async def test_bridge_mark_stopping_rejects_new_messages() -> None:
    handled: list[object] = []

    async def handler(message: object) -> None:
        handled.append(message)

    bridge = HyperliquidWsBridge(
        subscriptions=[make_subscription("trades", handler)],
        queue_maxsize=10,
        on_fatal=lambda: None,
    )
    await bridge.start()
    try:
        bridge.handle_open(0, asyncio.Event())
        bridge.mark_stopping()
        bridge.message_callback(0, "trades:eth")({"seq": 1})
        await asyncio.sleep(0.05)

        assert handled == []
        assert bridge.queues["trades:eth"].qsize() == 0
    finally:
        await bridge.stop()


async def _wait_for(predicate, *, interval: float = 0.01) -> None:
    while not predicate():
        await asyncio.sleep(interval)
