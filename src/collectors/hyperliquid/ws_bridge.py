from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from datetime import datetime, timezone

from .ws_subscription import HyperliquidWsSubscription
from .ws_support import (
    build_subscription_map,
    drain_queues,
    schedule_threadsafe,
)


class HyperliquidWsBridge:
    def __init__(
        self,
        *,
        subscriptions: Sequence[HyperliquidWsSubscription],
        queue_maxsize: int,
        on_fatal: Callable[[], None],
    ) -> None:
        self.subscriptions = build_subscription_map(subscriptions)
        self.queues = {
            key: asyncio.Queue(maxsize=queue_maxsize) for key in self.subscriptions
        }
        self.ready = asyncio.Event()
        self.failure: BaseException | None = None
        self.reconnect_requested = asyncio.Event()
        self.state = "idle"
        self.last_disconnect_reason: str | None = None
        self.last_error: str | None = None
        self.last_message_at: str | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._generation = 0
        self._on_fatal = on_fatal
        self._accepting_messages = False
        self._terminal = asyncio.Event()
        self._worker_tasks: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        if self._loop is not None:
            raise RuntimeError("HyperliquidWsBridge is already running")
        self._loop = asyncio.get_running_loop()
        self._terminal.clear()
        self.reconnect_requested.clear()
        self._accepting_messages = True
        self._worker_tasks = [
            asyncio.create_task(self._worker(key), name=f"hl-ws-worker:{key}")
            for key in self.subscriptions
        ]

    def begin_shutdown(self) -> None:
        self.state = "stopping"
        self.ready.clear()
        self._terminal.set()
        self._accepting_messages = False

    async def stop(self) -> None:
        self.begin_shutdown()
        for task in self._worker_tasks:
            task.cancel()
        await asyncio.gather(*self._worker_tasks, return_exceptions=True)
        self._worker_tasks.clear()
        self._loop = None
        self.state = "stopped"

    async def drain(self, timeout: float | None = None) -> bool:
        return await drain_queues(
            self.queues,
            has_failure=lambda: self.failure is not None,
            timeout=timeout,
        )

    def next_generation(self) -> int:
        self._generation += 1
        self.reconnect_requested.clear()
        return self._generation

    def is_current_generation(self, generation: int) -> bool:
        return generation == self._generation

    def threadsafe_callback(
        self, callback: Callable[..., None], *args: object
    ) -> Callable[..., None]:
        def wrapper(*runtime_args: object) -> None:
            schedule_threadsafe(self._loop, callback, *args, *runtime_args)

        return wrapper

    def handle_open(self, generation: int, opened: asyncio.Event) -> None:
        if (
            generation != self._generation
            or self.reconnect_requested.is_set()
            or self._terminal.is_set()
        ):
            return
        self._accepting_messages = True
        opened.set()
        self.ready.set()
        self.state = "connected"
        self.last_disconnect_reason = None
        self.last_error = None

    def handle_close(
        self,
        generation: int,
        closed: asyncio.Event,
        reason: str | None,
    ) -> None:
        if generation != self._generation:
            return
        self._accepting_messages = False
        if not self._terminal.is_set():
            self.state = "reconnecting"
        self.ready.clear()
        self.last_disconnect_reason = reason or "websocket_closed"
        closed.set()

    def handle_error(self, generation: int, error: BaseException) -> None:
        if generation != self._generation:
            return
        self._accepting_messages = False
        if not self._terminal.is_set():
            self.state = "reconnecting"
        self.ready.clear()
        self.last_disconnect_reason = "websocket_error"
        self.last_error = repr(error)

    def message_callback(self, generation: int, key: str) -> Callable[[object], None]:
        def callback(message: object) -> None:
            accepted = (
                generation == self._generation
                and self._accepting_messages
                and not self._terminal.is_set()
            )
            schedule_threadsafe(
                self._loop,
                self._queue_message,
                generation,
                key,
                accepted,
                message,
            )

        return callback

    async def _worker(self, key: str) -> None:
        queue = self.queues[key]
        handler = self.subscriptions[key].handler
        while True:
            message = await queue.get()
            try:
                await handler(message)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.failure = error
                self.last_error = repr(error)
                self._on_fatal()
                raise
            finally:
                queue.task_done()

    def request_reconnect(
        self,
        *,
        generation: int,
        reason: str,
        error: BaseException | None = None,
    ) -> None:
        if generation != self._generation or self._terminal.is_set():
            return
        self._accepting_messages = False
        self.state = "reconnecting"
        self.ready.clear()
        self.last_disconnect_reason = reason
        self.last_error = repr(error) if error is not None else None
        self.reconnect_requested.set()

    def _queue_message(
        self, generation: int, key: str, accepted: bool, message: object
    ) -> None:
        if not accepted:
            return
        self.last_message_at = datetime.now(timezone.utc).isoformat()
        try:
            self.queues[key].put_nowait(message)
        except asyncio.QueueFull:
            self.request_reconnect(
                generation=generation,
                reason=f"queue_overflow:{key}",
                error=RuntimeError(f"Hyperliquid queue overflow: {key}"),
            )
