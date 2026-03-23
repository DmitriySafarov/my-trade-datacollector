from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable, Sequence

from hyperliquid.websocket_manager import subscription_to_identifier

from .ws_subscription import HyperliquidWsSubscription


class HyperliquidWsNotReadyError(RuntimeError):
    """Raised when the manager stops before the first ready signal."""


class BoundedMessageSlots:
    def __init__(self, keys: Sequence[str], limit: int) -> None:
        self._counts = {key: 0 for key in keys}
        self._limit = limit
        self._lock = threading.Lock()

    def try_reserve(self, key: str) -> bool:
        with self._lock:
            if self._counts[key] >= self._limit:
                return False
            self._counts[key] += 1
            return True

    def release(self, key: str) -> None:
        with self._lock:
            if self._counts[key] > 0:
                self._counts[key] -= 1

    def is_empty(self) -> bool:
        with self._lock:
            return all(count == 0 for count in self._counts.values())

    def reset(self) -> None:
        with self._lock:
            for key in self._counts:
                self._counts[key] = 0


def build_health_snapshot(
    *,
    state: str,
    ready: bool,
    reconnect_count: int,
    last_disconnect_reason: str | None,
    last_error: str | None,
    last_message_at: str | None,
    current_gap_started_at: str | None,
    last_gap_started_at: str | None,
    last_gap_ended_at: str | None,
    last_gap_duration_seconds: float | None,
    subscriptions: dict[str, HyperliquidWsSubscription],
    queues: dict[str, asyncio.Queue[object]],
    subscription_health: dict[str, dict[str, object]],
    source_health: dict[str, dict[str, object]],
) -> dict[str, object]:
    return {
        "state": state,
        "ready": ready,
        "connected": state == "connected",
        "subscription_count": len(subscriptions),
        "reconnect_count": reconnect_count,
        "last_disconnect_reason": last_disconnect_reason,
        "last_error": last_error,
        "last_message_at": last_message_at,
        "current_gap_started_at": current_gap_started_at,
        "last_gap_started_at": last_gap_started_at,
        "last_gap_ended_at": last_gap_ended_at,
        "last_gap_duration_seconds": last_gap_duration_seconds,
        "queue_sizes": {key: queues[key].qsize() for key in subscriptions},
        "subscription_health": subscription_health,
        "source_health": source_health,
        "sources": source_health,
    }


def build_subscription_map(
    subscriptions: Sequence[HyperliquidWsSubscription],
) -> dict[str, HyperliquidWsSubscription]:
    if not subscriptions:
        raise ValueError("Hyperliquid subscriptions cannot be empty")
    mapping: dict[str, HyperliquidWsSubscription] = {}
    for definition in subscriptions:
        identifier = subscription_to_identifier(definition.subscription)
        if identifier in mapping:
            raise ValueError(f"duplicate Hyperliquid subscription: {identifier}")
        mapping[identifier] = definition
    return mapping


def schedule_threadsafe(
    loop: asyncio.AbstractEventLoop | None,
    callback: Callable[..., None],
    *args: object,
) -> bool:
    if loop is None or loop.is_closed():
        return False
    try:
        loop.call_soon_threadsafe(callback, *args)
    except RuntimeError:
        return False
    return True


async def wait_for_ready_or_terminal(
    ready: asyncio.Event,
    terminal: asyncio.Event,
) -> bool:
    ready_task = asyncio.create_task(ready.wait())
    terminal_task = asyncio.create_task(terminal.wait())
    done, pending = await asyncio.wait(
        {ready_task, terminal_task},
        return_when=asyncio.FIRST_COMPLETED,
    )
    del done
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    return ready.is_set()


def build_wait_ready_error(failure: BaseException | None) -> BaseException:
    return failure or HyperliquidWsNotReadyError(
        "Hyperliquid websocket stopped before reaching ready state"
    )


async def drain_queues(
    queues: dict[str, asyncio.Queue[object]],
    has_failure: Callable[[], bool],
    has_pending: Callable[[], bool] | None = None,
    timeout: float | None = None,
) -> bool:
    async def wait_for_queues() -> bool:
        joins = [asyncio.create_task(queue.join()) for queue in queues.values()]
        try:
            while not has_failure():
                done, _ = await asyncio.wait(joins, timeout=0.1)
                if len(done) == len(joins) and not (
                    has_pending is not None and has_pending()
                ):
                    return True
        finally:
            for task in joins:
                task.cancel()
            await asyncio.gather(*joins, return_exceptions=True)
        return False

    if timeout is None:
        return await wait_for_queues()
    try:
        async with asyncio.timeout(timeout):
            return await wait_for_queues()
    except TimeoutError:
        return False
