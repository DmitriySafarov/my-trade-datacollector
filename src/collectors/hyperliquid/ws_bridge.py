from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from datetime import datetime

from . import ws_bridge_support as support
from .ws_subscription import HyperliquidWsSubscription
from .ws_support import (
    BoundedMessageSlots,
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
        self._queue_maxsize = queue_maxsize
        self.queues = {
            key: asyncio.Queue(maxsize=queue_maxsize) for key in self.subscriptions
        }
        self._message_slots = BoundedMessageSlots(self.subscriptions, queue_maxsize)
        self.ready = asyncio.Event()
        self.failure: BaseException | None = None
        self.reconnect_requested = asyncio.Event()
        self.state = "idle"
        self.last_disconnect_reason = self.last_error = self.last_message_at = None
        self.last_gap_started_at = self.last_gap_ended_at = None
        self.last_gap_duration_seconds: float | None = None
        self.last_message_at_by_key = {key: None for key in self.subscriptions}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._generation = 0
        self._run_token = 0
        self._disconnect_reason_generation: int | None = None
        self._on_fatal = on_fatal
        self._accepting_messages = False
        self._terminal = asyncio.Event()
        self._worker_tasks: list[asyncio.Task[None]] = []
        self._current_gap_started_at: datetime | None = None

    async def start(self) -> None:
        if self._loop is not None:
            raise RuntimeError("HyperliquidWsBridge is already running")
        self._loop = asyncio.get_running_loop()
        self._terminal.clear()
        support.reset_run_state(self)
        self._worker_tasks = [
            asyncio.create_task(
                support.run_worker(self, key), name=f"hl-ws-worker:{key}"
            )
            for key in self.subscriptions
        ]

    def mark_stopping(self) -> None:
        self.state = "stopping"
        self.ready.clear()
        self._current_gap_started_at = None
        self._accepting_messages = False

    def begin_shutdown(self) -> None:
        self.mark_stopping()
        self._terminal.set()

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
            has_pending=lambda: not self._message_slots.is_empty(),
            timeout=timeout,
        )

    def next_generation(self) -> int:
        self._generation += 1
        self.reconnect_requested.clear()
        self._disconnect_reason_generation = None
        return self._generation

    def handle_open(self, generation: int, opened: asyncio.Event) -> None:
        support.handle_open(self, generation=generation, opened=opened)

    def handle_close(
        self, generation: int, closed: asyncio.Event, reason: str | None
    ) -> None:
        if generation != self._generation:
            return
        self._accepting_messages = False
        if self.state == "stopping" or self._terminal.is_set():
            closed.set()
            return
        support.enter_reconnecting_state(self)
        if not self._terminal.is_set():
            support.record_disconnect_reason(
                self,
                generation=generation,
                reason=reason or "websocket_closed",
            )
        closed.set()

    def handle_error(self, generation: int, error: BaseException) -> None:
        if generation != self._generation:
            return
        self._accepting_messages = False
        if self.state == "stopping" or self._terminal.is_set():
            return
        support.enter_reconnecting_state(self)
        if not self._terminal.is_set():
            support.record_disconnect_reason(
                self,
                generation=generation,
                reason="websocket_error",
            )
        self.last_error = repr(error)

    def message_callback(self, generation: int, key: str) -> Callable[[object], None]:
        run_token = self._run_token

        def callback(message: object) -> None:
            if not support.bridge_accepts_message(
                self,
                generation=generation,
                run_token=run_token,
            ):
                return
            if not self._message_slots.try_reserve(key):
                schedule_threadsafe(
                    self._loop,
                    lambda: self.request_reconnect(
                        generation=generation,
                        reason=f"queue_overflow:{key}",
                        error=RuntimeError(f"Hyperliquid queue overflow: {key}"),
                    ),
                )
                return
            if not schedule_threadsafe(
                self._loop,
                self._queue_message,
                generation,
                run_token,
                key,
                message,
            ):
                self._message_slots.release(key)

        return callback

    def request_reconnect(
        self,
        *,
        generation: int,
        reason: str,
        error: BaseException | None = None,
    ) -> None:
        if (
            generation != self._generation
            or self.state == "stopping"
            or self._terminal.is_set()
        ):
            return
        self._accepting_messages = False
        support.begin_gap(self)
        self.state = "reconnecting"
        self.ready.clear()
        support.record_disconnect_reason(self, generation=generation, reason=reason)
        self.last_error = repr(error) if error is not None else None
        self.reconnect_requested.set()

    def _queue_message(
        self,
        generation: int,
        run_token: int,
        key: str,
        message: object,
    ) -> None:
        support.queue_message(
            self,
            generation=generation,
            run_token=run_token,
            key=key,
            message=message,
        )
