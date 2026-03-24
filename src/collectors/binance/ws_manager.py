"""Binance Futures combined stream WebSocket manager.

Connects to ``wss://fstream.binance.com/stream?streams=...``,
routes incoming messages to registered handlers by stream name,
and reconnects with exponential backoff on disconnection.
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Sequence
from typing import Any

import aiohttp

from .ws_receive_support import MessageStats, receive_loop
from .ws_types import StreamConfig, StreamHandler

LOGGER = logging.getLogger(__name__)
DEFAULT_WS_URL = "wss://fstream.binance.com/stream"


class BinanceWsManager:
    """Binance Futures combined stream WebSocket with reconnect."""

    def __init__(
        self,
        *,
        streams: Sequence[StreamConfig],
        base_ws_url: str = DEFAULT_WS_URL,
        reconnect_base_seconds: float = 1.0,
        reconnect_max_seconds: float = 30.0,
        reconnect_jitter_ratio: float = 0.2,
        connect_timeout_seconds: float = 10.0,
        heartbeat_seconds: float = 20.0,
        max_startup_failures: int = 5,
    ) -> None:
        if not streams:
            raise ValueError("at least one stream is required")
        self._streams = tuple(streams)
        self._base_ws_url = base_ws_url.rstrip("/")
        self._reconnect_base = reconnect_base_seconds
        self._reconnect_max = reconnect_max_seconds
        self._reconnect_jitter = reconnect_jitter_ratio
        self._connect_timeout = connect_timeout_seconds
        self._heartbeat = heartbeat_seconds
        self._max_startup_failures = max_startup_failures
        self._handlers: dict[str, StreamHandler] = {
            s.stream_name: s.handler for s in self._streams
        }
        self._stop_event = asyncio.Event()
        self._ready_event = asyncio.Event()
        self._startup_error: BaseException | None = None
        self._current_ws: aiohttp.ClientWebSocketResponse | None = None
        self._stats = MessageStats()
        self._reconnect_count = 0
        self._connected = False

    @property
    def stream_count(self) -> int:
        return len(self._streams)

    def build_url(self) -> str:
        """Build the combined stream URL from registered stream names."""
        names = "/".join(s.stream_name for s in self._streams)
        return f"{self._base_ws_url}?streams={names}"

    async def run(self) -> None:
        """Main entry point — runs until stop() or startup failure."""
        timeout = aiohttp.ClientTimeout(total=None, connect=self._connect_timeout)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                await self._supervise(session)
        except BaseException:
            if not self._ready_event.is_set():
                if self._startup_error is None:
                    self._startup_error = RuntimeError(
                        "Binance WS terminated before becoming ready"
                    )
                self._ready_event.set()
            raise

    async def stop(self) -> None:
        """Signal the manager to stop and close current WS connection."""
        self._stop_event.set()
        ws = self._current_ws
        if ws is not None and not ws.closed:
            try:
                await asyncio.wait_for(ws.close(), timeout=5.0)
            except Exception:
                pass

    async def wait_ready(self) -> None:
        """Block until the first WS connection succeeds or startup fails."""
        await self._ready_event.wait()
        if self._startup_error is not None:
            raise self._startup_error

    def health_snapshot(self) -> dict[str, Any]:
        return {
            "connected": self._connected,
            "reconnect_count": self._reconnect_count,
            "total_messages": self._stats.total_messages,
            "total_routing_errors": self._stats.total_routing_errors,
            "last_message_at": self._stats.last_message_at,
            "last_error_at": self._stats.last_error_at,
            "last_error_message": self._stats.last_error_msg,
            "stream_count": len(self._streams),
            "streams": [s.stream_name for s in self._streams],
        }

    async def _supervise(self, session: aiohttp.ClientSession) -> None:
        url = self.build_url()
        consecutive_failures = 0
        ready_signalled = False
        while not self._stop_event.is_set():
            try:
                ws = await session.ws_connect(url, heartbeat=self._heartbeat)
            except Exception as error:
                consecutive_failures += 1
                self._stats.record_error(error)
                LOGGER.warning(
                    "binance_ws_connect_failed attempt=%d error=%r",
                    consecutive_failures,
                    error,
                )
                if (
                    not ready_signalled
                    and consecutive_failures >= self._max_startup_failures
                ):
                    err = RuntimeError(
                        f"Binance WS failed {consecutive_failures} consecutive "
                        f"connection attempts during startup"
                    )
                    err.__cause__ = error
                    self._startup_error = err
                    self._ready_event.set()
                    raise err
                delay = self._backoff(consecutive_failures)
                if await self._sleep_or_stop(delay):
                    return
                continue
            self._current_ws = ws
            self._connected = True
            consecutive_failures = 0
            if not ready_signalled:
                self._ready_event.set()
                ready_signalled = True
            LOGGER.info("binance_ws_connected streams=%d", len(self._streams))
            try:
                await receive_loop(ws, self._handlers, self._stats, self._stop_event)
            except Exception as error:
                self._stats.record_error(error)
                LOGGER.warning("binance_ws_receive_error error=%r", error)
            finally:
                self._connected = False
                self._current_ws = None
                if not ws.closed:
                    await ws.close()
            if self._stop_event.is_set():
                return
            self._reconnect_count += 1
            delay = self._backoff(0)
            LOGGER.warning(
                "binance_ws_reconnecting count=%d delay=%.2f",
                self._reconnect_count,
                delay,
            )
            if await self._sleep_or_stop(delay):
                return

    def _backoff(self, attempt: int) -> float:
        base = min(self._reconnect_base * (2**attempt), self._reconnect_max)
        return base + random.uniform(0, base * self._reconnect_jitter)

    async def _sleep_or_stop(self, seconds: float) -> bool:
        """Sleep, returning True if stop was requested during sleep."""
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
            return True
        except asyncio.TimeoutError:
            return False
