from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence

from hyperliquid.websocket_manager import subscription_to_identifier

from .ws_subscription import HyperliquidWsSubscription


class HyperliquidWsNotReadyError(RuntimeError):
    """Raised when the manager stops before the first ready signal."""


def build_health_snapshot(
    *,
    state: str,
    ready: bool,
    reconnect_count: int,
    last_disconnect_reason: str | None,
    last_error: str | None,
    last_message_at: str | None,
    subscriptions: dict[str, HyperliquidWsSubscription],
    queues: dict[str, asyncio.Queue[object]],
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
        "queue_sizes": {key: queues[key].qsize() for key in subscriptions},
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
) -> None:
    if loop is not None and not loop.is_closed():
        loop.call_soon_threadsafe(callback, *args)


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
    timeout: float | None = None,
) -> bool:
    async def wait_for_queues() -> bool:
        joins = [asyncio.create_task(queue.join()) for queue in queues.values()]
        try:
            while not has_failure():
                done, _ = await asyncio.wait(joins, timeout=0.1)
                if len(done) == len(joins):
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
