from __future__ import annotations

import asyncio

import pytest

from src.collectors.hyperliquid import HyperliquidWsManager

from ._ws_test_support import (
    FakeHyperliquidSession,
    FakeHyperliquidSessionFactory,
    make_subscription,
)


class _OrderingSession(FakeHyperliquidSession):
    def __init__(
        self,
        on_open,
        on_close,
        on_error,
        *,
        expected_subscriptions: int,
    ) -> None:
        super().__init__(on_open, on_close, on_error)
        self.expected_subscriptions = expected_subscriptions

    def start(self) -> None:
        if len(self.subscriptions) != self.expected_subscriptions:
            raise RuntimeError(
                "session started before all subscriptions were registered"
            )
        super().start()
        self.open()


def test_manager_requires_at_least_one_subscription() -> None:
    with pytest.raises(ValueError, match="cannot be empty"):
        HyperliquidWsManager(
            base_url="https://api.hyperliquid.xyz",
            subscriptions=[],
            session_factory=FakeHyperliquidSessionFactory(),
        )


@pytest.mark.asyncio
async def test_manager_waits_ready_and_dispatches_messages() -> None:
    factory = FakeHyperliquidSessionFactory()
    received: list[object] = []

    async def handler(message: object) -> None:
        received.append(message)

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

    await asyncio.wait_for(_wait_for(lambda: bool(received)), timeout=1.0)
    assert received == [{"channel": "trades", "data": [{"coin": "ETH"}]}]
    assert manager.health_snapshot()["connected"] is True
    assert "trades:eth" in manager.health_snapshot()["queue_sizes"]

    await manager.stop()
    await asyncio.wait_for(task, timeout=1.0)
    assert session.closed is True


@pytest.mark.asyncio
async def test_manager_registers_all_subscriptions_before_session_start() -> None:
    factory = FakeHyperliquidSessionFactory(
        session_builder=lambda on_open, on_close, on_error: _OrderingSession(
            on_open,
            on_close,
            on_error,
            expected_subscriptions=2,
        )
    )

    async def handler(_message: object) -> None:
        return None

    manager = HyperliquidWsManager(
        base_url="https://api.hyperliquid.xyz",
        subscriptions=[
            make_subscription("eth-trades", handler, coin="ETH"),
            make_subscription("btc-trades", handler, coin="BTC"),
        ],
        session_factory=factory,
    )
    task = asyncio.create_task(manager.run())
    session = await factory.next_session()

    await manager.wait_ready()
    assert set(session.subscriptions) == {"trades:eth", "trades:btc"}

    await manager.stop()
    await asyncio.wait_for(task, timeout=1.0)


@pytest.mark.asyncio
async def test_manager_reconnects_and_clears_ready_after_disconnect() -> None:
    factory = FakeHyperliquidSessionFactory()
    received: list[object] = []

    async def handler(message: object) -> None:
        received.append(message)

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
    first.disconnect("network_drop")
    await asyncio.wait_for(
        _wait_for(lambda: manager.health_snapshot()["ready"] is False),
        timeout=1.0,
    )
    assert manager.health_snapshot()["state"] == "reconnecting"

    second = await asyncio.wait_for(factory.next_session(), timeout=1.0)
    second.open()
    await asyncio.wait_for(
        _wait_for(lambda: manager.health_snapshot()["reconnect_count"] == 1),
        timeout=1.0,
    )
    await manager.wait_ready()
    second.emit("trades:eth", {"channel": "trades", "data": [{"coin": "BTC"}]})
    await asyncio.wait_for(_wait_for(lambda: len(received) == 1), timeout=1.0)
    assert "trades:eth" in second.subscriptions
    assert received == [{"channel": "trades", "data": [{"coin": "BTC"}]}]

    await manager.stop()
    await asyncio.wait_for(task, timeout=1.0)


@pytest.mark.asyncio
async def test_manager_records_gap_window_after_reconnect() -> None:
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
    first.disconnect("network_drop")
    await asyncio.wait_for(factory.next_session(), timeout=1.0)
    await asyncio.sleep(0.02)
    factory.sessions[-1].open()
    await manager.wait_ready()

    snapshot = manager.health_snapshot()
    assert snapshot["current_gap_started_at"] is None
    assert snapshot["last_gap_started_at"] is not None
    assert snapshot["last_gap_ended_at"] is not None
    assert snapshot["last_gap_duration_seconds"] >= 0

    await manager.stop()
    await asyncio.wait_for(task, timeout=1.0)


async def _wait_for(predicate, *, interval: float = 0.01) -> None:
    while not predicate():
        await asyncio.sleep(interval)
