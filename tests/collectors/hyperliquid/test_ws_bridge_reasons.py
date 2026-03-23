from __future__ import annotations

import asyncio
import threading

import pytest

from src.collectors.hyperliquid import HyperliquidWsManager
from src.collectors.hyperliquid.ws_bridge import HyperliquidWsBridge

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
async def test_cleanup_close_keeps_existing_reconnect_reason() -> None:
    factory = FakeHyperliquidSessionFactory(session_builder=_ClosingCallbackSession)
    release = asyncio.Event()
    producer_done = threading.Event()
    handled: list[int] = []

    async def handler(message: object) -> None:
        handled.append(message["seq"])
        await release.wait()

    manager = HyperliquidWsManager(
        base_url="https://api.hyperliquid.xyz",
        subscriptions=[make_subscription("trades", handler)],
        queue_maxsize=1,
        reconnect_base_seconds=0.01,
        reconnect_max_seconds=0.01,
        reconnect_jitter_ratio=0.0,
        session_factory=factory,
    )
    task = asyncio.create_task(manager.run())
    first = await factory.next_session()

    first.open()
    await manager.wait_ready()

    def produce() -> None:
        first.emit("trades:eth", {"seq": 1})
        first.emit("trades:eth", {"seq": 2})
        first.emit("trades:eth", {"seq": 3})
        producer_done.set()

    producer = threading.Thread(target=produce, daemon=True)
    producer.start()

    await asyncio.wait_for(_wait_for(lambda: handled == [1]), timeout=1.0)
    await asyncio.wait_for(asyncio.to_thread(producer.join, 1.0), timeout=2.0)
    await asyncio.wait_for(factory.next_session(), timeout=1.0)

    assert producer_done.is_set() is True
    assert (
        manager.health_snapshot()["last_disconnect_reason"]
        == "queue_overflow:trades:eth"
    )
    assert (
        "Hyperliquid queue overflow: trades:eth"
        in manager.health_snapshot()["last_error"]
    )

    release.set()
    await manager.stop()
    await asyncio.wait_for(task, timeout=1.0)


@pytest.mark.asyncio
async def test_late_error_keeps_existing_reconnect_reason() -> None:
    factory = FakeHyperliquidSessionFactory(session_builder=_ClosingCallbackSession)
    release = asyncio.Event()

    async def handler(_message: object) -> None:
        await release.wait()

    manager = HyperliquidWsManager(
        base_url="https://api.hyperliquid.xyz",
        subscriptions=[make_subscription("trades", handler)],
        queue_maxsize=1,
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
    first.emit("trades:eth", {"seq": 2})
    await asyncio.wait_for(
        _wait_for(manager._bridge.reconnect_requested.is_set), timeout=1.0
    )

    first.fail(RuntimeError("late boom"))
    await asyncio.wait_for(factory.next_session(), timeout=1.0)

    assert (
        manager.health_snapshot()["last_disconnect_reason"]
        == "queue_overflow:trades:eth"
    )
    assert manager.health_snapshot()["last_error"] == "RuntimeError('late boom')"

    release.set()
    await manager.stop()
    await asyncio.wait_for(task, timeout=1.0)


@pytest.mark.asyncio
async def test_first_worker_failure_is_preserved() -> None:
    second_started = asyncio.Event()
    release_second = asyncio.Event()

    async def first_handler(_message: object) -> None:
        raise RuntimeError("boom-1")

    async def second_handler(_message: object) -> None:
        second_started.set()
        await release_second.wait()
        raise RuntimeError("boom-2")

    bridge = HyperliquidWsBridge(
        subscriptions=[
            make_subscription("trades", first_handler, coin="ETH"),
            make_subscription("trades", second_handler, coin="BTC"),
        ],
        queue_maxsize=10,
        on_fatal=lambda: None,
    )
    await bridge.start()
    try:
        bridge.handle_open(0, asyncio.Event())
        bridge.message_callback(0, "trades:eth")({"seq": 1})
        bridge.message_callback(0, "trades:btc")({"seq": 2})
        await asyncio.wait_for(second_started.wait(), timeout=1.0)
        await asyncio.wait_for(
            _wait_for(lambda: bridge.failure is not None), timeout=1.0
        )
        release_second.set()
        await asyncio.sleep(0.05)

        assert repr(bridge.failure) == "RuntimeError('boom-1')"
        assert any(
            "boom-2" in note for note in getattr(bridge.failure, "__notes__", ())
        )
    finally:
        await bridge.stop()


@pytest.mark.asyncio
async def test_overflow_drop_does_not_refresh_freshness() -> None:
    factory = FakeHyperliquidSessionFactory()
    release = asyncio.Event()
    handled: list[int] = []

    async def handler(message: object) -> None:
        handled.append(message["seq"])
        await release.wait()

    manager = HyperliquidWsManager(
        base_url="https://api.hyperliquid.xyz",
        subscriptions=[make_subscription("trades", handler)],
        queue_maxsize=1,
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
    await asyncio.wait_for(_wait_for(lambda: handled == [1]), timeout=1.0)
    last_message_at = manager.health_snapshot()["last_message_at"]

    await asyncio.sleep(0.02)
    first.emit("trades:eth", {"seq": 2})
    await asyncio.wait_for(
        _wait_for(manager._bridge.reconnect_requested.is_set), timeout=1.0
    )

    assert manager.health_snapshot()["last_message_at"] == last_message_at

    release.set()
    await manager.stop()
    await asyncio.wait_for(task, timeout=1.0)


async def _wait_for(predicate, *, interval: float = 0.01) -> None:
    while not predicate():
        await asyncio.sleep(interval)
