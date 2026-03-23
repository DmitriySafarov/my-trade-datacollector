from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field

from hyperliquid.websocket_manager import subscription_to_identifier
from src.collectors.hyperliquid import HyperliquidWsSubscription


@dataclass
class FakeHyperliquidSession:
    on_open: Callable[[], None]
    on_close: Callable[[str | None], None]
    on_error: Callable[[BaseException], None]
    started: bool = False
    closed: bool = False
    subscriptions: dict[str, Callable[[object], None]] = field(default_factory=dict)

    def start(self) -> None:
        self.started = True

    def subscribe(self, subscription, callback: Callable[[object], None]) -> None:
        self.subscriptions[subscription_to_identifier(subscription)] = callback

    def close(self) -> None:
        self.closed = True

    def is_alive(self) -> bool:
        return self.started and not self.closed

    def open(self) -> None:
        self.on_open()

    def emit(self, identifier: str, message: object) -> None:
        self.subscriptions[identifier](message)

    def disconnect(self, reason: str = "test_disconnect") -> None:
        self.closed = True
        self.on_close(reason)

    def fail(self, error: BaseException) -> None:
        self.on_error(error)


class FakeHyperliquidSessionFactory:
    def __init__(
        self, session_builder: Callable[..., FakeHyperliquidSession] | None = None
    ) -> None:
        self.sessions: list[FakeHyperliquidSession] = []
        self._created = asyncio.Queue()
        self._session_builder = session_builder

    def __call__(self, base_url, on_open, on_close, on_error) -> FakeHyperliquidSession:
        del base_url
        builder = self._session_builder or FakeHyperliquidSession
        session = builder(on_open, on_close, on_error)
        self.sessions.append(session)
        self._created.put_nowait(session)
        return session

    async def next_session(self) -> FakeHyperliquidSession:
        return await self._created.get()


def make_subscription(name: str, handler, *, coin: str = "ETH"):
    return HyperliquidWsSubscription(
        name=name,
        subscription={"type": "trades", "coin": coin},
        handler=handler,
    )
