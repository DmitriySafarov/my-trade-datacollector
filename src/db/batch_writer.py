from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Generic, TypeVar


T = TypeVar("T")
FlushCallback = Callable[[list[T]], Awaitable[None]]


class BatchWriter(Generic[T]):
    def __init__(
        self,
        name: str,
        count_limit: int,
        time_limit_seconds: float,
        flush_callback: FlushCallback[T],
    ) -> None:
        self.name = name
        self.count_limit = count_limit
        self.time_limit_seconds = time_limit_seconds
        self.flush_callback = flush_callback
        self._buffer: list[T] = []
        self._state_lock = asyncio.Lock()
        self._flush_task: asyncio.Task[None] | None = None
        self._timer_task: asyncio.Task[None] | None = None
        self._closed = False
        self._failure: BaseException | None = None
        self._failure_event = asyncio.Event()

    async def add(self, record: T) -> None:
        await self.add_many([record])

    async def add_many(self, records: list[T]) -> None:
        self._raise_if_failed()
        async with self._state_lock:
            self._raise_if_failed()
            if self._closed:
                raise RuntimeError(f"BatchWriter {self.name} is closed")
            was_empty = not self._buffer
            self._buffer.extend(records)
            should_flush = len(self._buffer) >= self.count_limit
            if should_flush:
                task = self._ensure_flush_task_locked()
            else:
                task = None
                if was_empty and self._flush_task is None:
                    self._schedule_timer_locked()
        if task is not None:
            await asyncio.shield(task)

    async def flush(self) -> None:
        async with self._state_lock:
            task = self._flush_task
            if self._buffer:
                task = task or self._ensure_flush_task_locked()
        if task is not None:
            await asyncio.shield(task)

    async def close(self) -> None:
        async with self._state_lock:
            self._closed = True
            self._cancel_timer_locked()
            task = self._flush_task
            if self._buffer:
                task = task or self._ensure_flush_task_locked()
        if task is not None:
            await asyncio.shield(task)

    async def abort(self) -> None:
        async with self._state_lock:
            self._closed = True
            self._cancel_timer_locked()
            task = self._flush_task
            if task is not None:
                task.cancel()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)

    async def wait_failure(self) -> None:
        await self._failure_event.wait()
        assert self._failure is not None
        raise self._failure

    @property
    def failure(self) -> BaseException | None:
        return self._failure

    def snapshot(self) -> dict[str, float | int | str]:
        return {
            "name": self.name,
            "buffer_size": len(self._buffer),
            "count_limit": self.count_limit,
            "time_limit_seconds": self.time_limit_seconds,
            "last_error": repr(self._failure) if self._failure is not None else "",
        }

    def _ensure_flush_task_locked(self) -> asyncio.Task[None]:
        if self._flush_task is None:
            self._cancel_timer_locked()
            self._flush_task = asyncio.create_task(self._flush_loop())
        return self._flush_task

    async def _flush_loop(self) -> None:
        task = asyncio.current_task()
        try:
            while True:
                async with self._state_lock:
                    if not self._buffer:
                        self._flush_task = None
                        return
                    payload = self._buffer
                    self._buffer = []
                    self._cancel_timer_locked()
                await self.flush_callback(payload)
        except BaseException as error:
            async with self._state_lock:
                if "payload" in locals():
                    self._buffer = [*payload, *self._buffer]
                    if not self._closed and self._timer_task is None:
                        self._schedule_timer_locked()
                if self._flush_task is task:
                    self._flush_task = None
            self._record_failure(error)
            raise

    def _schedule_timer_locked(self) -> None:
        self._cancel_timer_locked()
        self._timer_task = asyncio.create_task(self._timer_flush())

    def _cancel_timer_locked(self) -> None:
        if self._timer_task is None:
            return
        self._timer_task.cancel()
        self._timer_task = None

    async def _timer_flush(self) -> None:
        task = asyncio.current_task()
        try:
            await asyncio.sleep(self.time_limit_seconds)
            async with self._state_lock:
                if self._timer_task is task:
                    self._timer_task = None
                flush_task = self._flush_task
                if self._buffer:
                    flush_task = flush_task or self._ensure_flush_task_locked()
            if flush_task is not None:
                await flush_task
        except asyncio.CancelledError:
            return
        except BaseException as error:
            self._record_failure(error)

    def _record_failure(self, error: BaseException) -> None:
        if self._failure is not None:
            return
        self._failure = error
        self._failure_event.set()

    def _raise_if_failed(self) -> None:
        if self._failure is None:
            return
        raise RuntimeError(f"BatchWriter {self.name} has failed") from self._failure
