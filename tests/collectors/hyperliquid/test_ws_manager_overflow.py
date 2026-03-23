from __future__ import annotations

import asyncio
import threading

import pytest

from src.collectors.hyperliquid import HyperliquidWsManager

from ._ws_test_support import FakeHyperliquidSessionFactory, make_subscription


@pytest.mark.asyncio
async def test_queue_overflow_requests_reconnect_without_blocking_producer() -> None:
    factory = FakeHyperliquidSessionFactory()
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
    second = await asyncio.wait_for(factory.next_session(), timeout=1.0)
    assert producer_done.is_set() is True
    assert (
        manager.health_snapshot()["last_disconnect_reason"]
        == "queue_overflow:trades:eth"
    )
    assert manager.health_snapshot()["reconnect_count"] == 1

    release.set()
    second.open()
    await manager.wait_ready()
    second.emit("trades:eth", {"seq": 4})
    await asyncio.wait_for(_wait_for(lambda: handled[-1:] == [4]), timeout=1.0)

    await manager.stop()
    await asyncio.wait_for(task, timeout=1.0)


async def _wait_for(predicate, *, interval: float = 0.01) -> None:
    while not predicate():
        await asyncio.sleep(interval)
