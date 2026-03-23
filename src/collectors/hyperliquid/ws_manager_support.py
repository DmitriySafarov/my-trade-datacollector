from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Callable
from typing import TYPE_CHECKING

from .ws_support import (
    build_health_snapshot,
    build_wait_ready_error,
    wait_for_ready_or_terminal,
)
from .ws_session import HyperliquidWsSession

if TYPE_CHECKING:
    from .ws_bridge import HyperliquidWsBridge


def compute_reconnect_delay(
    *,
    base_seconds: float,
    max_seconds: float,
    attempt: int,
    jitter_ratio: float,
) -> float:
    delay = min(base_seconds * (2**attempt), max_seconds)
    if delay <= 0 or jitter_ratio <= 0:
        return delay
    spread = delay * jitter_ratio
    return random.uniform(max(0.0, delay - spread), delay + spread)


async def wait_bridge_ready(bridge: HyperliquidWsBridge) -> None:
    if bridge.ready.is_set() or await wait_for_ready_or_terminal(
        bridge.ready, bridge._terminal
    ):
        return
    raise build_wait_ready_error(bridge.failure)


def build_bridge_health_snapshot(
    bridge: HyperliquidWsBridge, reconnect_count: int
) -> dict[str, object]:
    return build_health_snapshot(
        state=bridge.state,
        ready=bridge.ready.is_set(),
        reconnect_count=reconnect_count,
        last_disconnect_reason=bridge.last_disconnect_reason,
        last_error=bridge.last_error,
        last_message_at=bridge.last_message_at,
        subscriptions=bridge.subscriptions,
        queues=bridge.queues,
    )


async def wait_for_disconnect(
    *,
    session: HyperliquidWsSession,
    opened: asyncio.Event,
    closed: asyncio.Event,
    errored: asyncio.Event,
    reconnect_requested: asyncio.Event,
    stop_event: asyncio.Event,
    has_failure: Callable[[], bool],
    open_timeout_seconds: float,
    on_open_timeout: Callable[[], None],
    on_session_exit: Callable[[], None],
) -> None:
    deadline = time.monotonic() + open_timeout_seconds
    while not stop_event.is_set() and not has_failure():
        if closed.is_set() or errored.is_set() or reconnect_requested.is_set():
            return
        if not opened.is_set() and time.monotonic() >= deadline:
            on_open_timeout()
            return
        if not session.is_alive():
            on_session_exit()
            return
        await asyncio.sleep(0.2)


async def wait_for_stop_or_timeout(stop_event: asyncio.Event, delay: float) -> bool:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=delay)
    except asyncio.TimeoutError:
        return False
    return True


def handle_session_error(
    bridge: HyperliquidWsBridge,
    generation: int,
    errored: asyncio.Event,
    error: BaseException,
) -> None:
    bridge.handle_error(generation, error)
    if bridge.is_current_generation(generation):
        errored.set()
