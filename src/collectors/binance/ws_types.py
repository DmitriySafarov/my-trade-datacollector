"""Types for Binance WebSocket stream handling."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass

StreamHandler = Callable[[Mapping[str, object]], Awaitable[None]]
"""Async callback receiving the ``data`` payload from a combined stream message."""


@dataclass(frozen=True, slots=True)
class StreamConfig:
    """Configuration for a single Binance combined stream subscription.

    The ``stream_name`` follows the Binance combined stream naming convention,
    e.g. ``ethusdt@aggTrade``, ``btcusdt@depth20@100ms``.
    """

    stream_name: str
    handler: StreamHandler
