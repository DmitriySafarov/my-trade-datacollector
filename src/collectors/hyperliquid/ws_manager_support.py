from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Callable
from typing import TYPE_CHECKING

from . import ws_health_support as health
from .ws_support import (
    build_health_snapshot,
    build_wait_ready_error,
    schedule_threadsafe,
    wait_for_ready_or_terminal,
)
from .ws_session import HyperliquidWsSession, SessionFactory
from .ws_close_support import close_manager_session

if TYPE_CHECKING:
    from .ws_bridge import HyperliquidWsBridge
    from .ws_manager import HyperliquidWsManager


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
    if not (
        bridge.ready.is_set()
        or await wait_for_ready_or_terminal(bridge.ready, bridge._terminal)
    ):
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
        current_gap_started_at=(
            bridge._current_gap_started_at.isoformat()
            if bridge._current_gap_started_at is not None
            else None
        ),
        last_gap_started_at=bridge.last_gap_started_at,
        last_gap_ended_at=bridge.last_gap_ended_at,
        last_gap_duration_seconds=bridge.last_gap_duration_seconds,
        subscriptions=bridge.subscriptions,
        queues=bridge.queues,
        subscription_health=health.build_subscription_health(bridge),
        source_health=health.build_source_health(bridge),
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


async def drain_bridge(
    bridge: HyperliquidWsBridge,
    *,
    timeout: float | None,
    logger: logging.Logger | None = None,
) -> None:
    drained = await bridge.drain(timeout=timeout)
    if bridge.failure is not None:
        raise bridge.failure
    if timeout is None or drained:
        return
    if logger is not None:
        logger.warning(
            "hyperliquid_ws_shutdown_drain_timeout timeout_seconds=%.2f",
            timeout,
        )
    raise RuntimeError("Hyperliquid websocket shutdown drain timed out")


async def drain_after_close(
    manager: HyperliquidWsManager, *, bridge_drained: bool
) -> None:
    if bridge_drained or manager._bridge.failure is not None:
        return
    await drain_bridge(manager._bridge, timeout=manager.shutdown_drain_timeout_seconds)


async def cleanup_setup_failure(
    manager: HyperliquidWsManager,
    *,
    generation: int,
    error: BaseException,
    logger: logging.Logger,
    timeout_seconds: float,
) -> None:
    manager._bridge.handle_error(generation, error)
    manager._bridge.last_disconnect_reason = f"setup_failed:{type(error).__name__}"
    await close_manager_session(
        manager,
        reason="setup_cleanup",
        timeout_seconds=timeout_seconds,
        logger=logger,
    )


def open_session(
    *,
    session_factory: SessionFactory,
    base_url: str,
    bridge: HyperliquidWsBridge,
    generation: int,
    opened: asyncio.Event,
    closed: asyncio.Event,
    errored: asyncio.Event,
) -> HyperliquidWsSession:
    session = session_factory(
        base_url,
        threadsafe_callback(bridge, bridge.handle_open, generation, opened),
        threadsafe_callback(bridge, bridge.handle_close, generation, closed),
        threadsafe_callback(
            bridge,
            handle_session_error,
            bridge,
            generation,
            errored,
        ),
    )
    for key, definition in bridge.subscriptions.items():
        session.subscribe(
            definition.subscription, bridge.message_callback(generation, key)
        )
    return session


def handle_session_error(
    bridge: HyperliquidWsBridge,
    generation: int,
    errored: asyncio.Event,
    error: BaseException,
) -> None:
    bridge.handle_error(generation, error)
    if generation == bridge._generation:
        errored.set()


def threadsafe_callback(
    bridge: HyperliquidWsBridge,
    callback: Callable[..., None],
    *args: object,
) -> Callable[..., None]:
    def wrapper(*runtime_args: object) -> None:
        schedule_threadsafe(bridge._loop, callback, *args, *runtime_args)

    return wrapper
