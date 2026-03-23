from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from hyperliquid.utils.types import Subscription, WsMsg


MessageHandler = Callable[[WsMsg], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class HyperliquidWsSubscription:
    name: str
    subscription: Subscription
    handler: MessageHandler
