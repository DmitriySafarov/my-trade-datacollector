from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .ws_bridge import HyperliquidWsBridge


def reset_run_state(bridge: HyperliquidWsBridge) -> None:
    bridge._run_token += 1
    bridge.queues = {
        key: asyncio.Queue(maxsize=bridge._queue_maxsize)
        for key in bridge.subscriptions
    }
    bridge._message_slots.reset()
    bridge.ready.clear()
    bridge.failure = None
    bridge.reconnect_requested.clear()
    bridge.state = "idle"
    bridge._disconnect_reason_generation = None
    bridge.last_disconnect_reason = None
    bridge.last_error = None
    bridge.last_message_at = None
    bridge.last_gap_started_at = None
    bridge.last_gap_ended_at = None
    bridge.last_gap_duration_seconds = None
    bridge._current_gap_started_at = None
    bridge._accepting_messages = False
    for key in bridge.last_message_at_by_key:
        bridge.last_message_at_by_key[key] = None


def handle_open(
    bridge: HyperliquidWsBridge,
    *,
    generation: int,
    opened: asyncio.Event,
) -> None:
    if (
        generation != bridge._generation
        or bridge.reconnect_requested.is_set()
        or bridge._terminal.is_set()
        or bridge.state == "stopping"
    ):
        return
    bridge._accepting_messages = True
    complete_gap(bridge)
    opened.set()
    bridge.ready.set()
    bridge.state = "connected"


def record_disconnect_reason(
    bridge: HyperliquidWsBridge,
    *,
    generation: int,
    reason: str,
) -> None:
    if generation != bridge._generation:
        return
    if bridge._disconnect_reason_generation == generation:
        return
    bridge._disconnect_reason_generation = generation
    bridge.last_disconnect_reason = reason


def bridge_accepts_message(
    bridge: HyperliquidWsBridge,
    *,
    generation: int,
    run_token: int,
) -> bool:
    return (
        generation == bridge._generation
        and run_token == bridge._run_token
        and bridge._accepting_messages
        and bridge.state != "stopping"
        and not bridge._terminal.is_set()
    )


def queue_message(
    bridge: HyperliquidWsBridge,
    *,
    generation: int,
    run_token: int,
    key: str,
    message: object,
) -> None:
    if (
        run_token != bridge._run_token
        or bridge._terminal.is_set()
        or bridge.failure is not None
    ):
        bridge._message_slots.release(key)
        return
    try:
        bridge.queues[key].put_nowait(message)
    except asyncio.QueueFull:
        bridge._message_slots.release(key)
        bridge.request_reconnect(
            generation=generation,
            reason=f"queue_overflow:{key}",
            error=RuntimeError(f"Hyperliquid queue overflow: {key}"),
        )
    else:
        timestamp = datetime.now(timezone.utc).isoformat()
        bridge.last_message_at = timestamp
        bridge.last_message_at_by_key[key] = timestamp


def begin_gap(bridge: HyperliquidWsBridge) -> None:
    if bridge._terminal.is_set() or bridge._current_gap_started_at is not None:
        return
    bridge._current_gap_started_at = datetime.now(timezone.utc)


def enter_reconnecting_state(bridge: HyperliquidWsBridge) -> None:
    bridge._accepting_messages = False
    begin_gap(bridge)
    if not bridge._terminal.is_set():
        bridge.state = "reconnecting"
    bridge.ready.clear()


def complete_gap(bridge: HyperliquidWsBridge) -> None:
    gap_started_at = bridge._current_gap_started_at
    if gap_started_at is None:
        return
    gap_ended_at = datetime.now(timezone.utc)
    bridge.last_gap_started_at = gap_started_at.isoformat()
    bridge.last_gap_ended_at = gap_ended_at.isoformat()
    bridge.last_gap_duration_seconds = max(
        0.0,
        (gap_ended_at - gap_started_at).total_seconds(),
    )
    bridge._current_gap_started_at = None


async def run_worker(bridge: HyperliquidWsBridge, key: str) -> None:
    queue = bridge.queues[key]
    handler = bridge.subscriptions[key].handler
    while True:
        message = await queue.get()
        try:
            await handler(message)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if bridge.failure is None:
                bridge.failure = error
                bridge.last_error = repr(error)
            elif error is not bridge.failure:
                bridge.failure.add_note(f"Additional worker failure: {error!r}")
            bridge._on_fatal()
            raise
        finally:
            queue.task_done()
            bridge._message_slots.release(key)
