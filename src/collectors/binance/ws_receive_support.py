"""Receive loop and message routing for Binance combined stream.

Extracted from ``ws_manager.py`` to keep both files under 200 lines.
The receive loop reads WebSocket messages and routes them to
registered handlers by stream name, tracking delivery statistics.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Mapping
from time import monotonic

import aiohttp

from .ws_types import StreamHandler

LOGGER = logging.getLogger(__name__)


class MessageStats:
    """Mutable counters for message routing statistics."""

    __slots__ = (
        "total_messages",
        "total_routing_errors",
        "last_message_at",
        "last_error_at",
        "last_error_msg",
    )

    def __init__(self) -> None:
        self.total_messages = 0
        self.total_routing_errors = 0
        self.last_message_at: float | None = None
        self.last_error_at: float | None = None
        self.last_error_msg: str = ""

    def record_error(self, error: BaseException) -> None:
        self.last_error_at = monotonic()
        self.last_error_msg = repr(error)


async def receive_loop(
    ws: aiohttp.ClientWebSocketResponse,
    handlers: dict[str, StreamHandler],
    stats: MessageStats,
    stop_event: asyncio.Event,
) -> None:
    """Read messages from WS and route to handlers until closed or stopped."""
    async for msg in ws:
        if stop_event.is_set():
            return
        if msg.type == aiohttp.WSMsgType.TEXT:
            await handle_text(msg.data, handlers, stats)
        elif msg.type in (
            aiohttp.WSMsgType.ERROR,
            aiohttp.WSMsgType.CLOSED,
            aiohttp.WSMsgType.CLOSE,
        ):
            return


async def handle_text(
    raw: str | bytes,
    handlers: dict[str, StreamHandler],
    stats: MessageStats,
) -> None:
    """Parse a combined stream TEXT message and dispatch to its handler."""
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, TypeError) as error:
        stats.record_error(error)
        return
    if not isinstance(payload, dict):
        return
    stream, data = payload.get("stream"), payload.get("data")
    if not isinstance(stream, str) or not isinstance(data, Mapping):
        return
    handler = handlers.get(stream)
    if handler is None:
        return
    try:
        await handler(data)
    except Exception as error:
        stats.total_routing_errors += 1
        stats.record_error(error)
        LOGGER.warning("binance_ws_handler_error stream=%s error=%r", stream, error)
        return
    stats.total_messages += 1
    stats.last_message_at = monotonic()
