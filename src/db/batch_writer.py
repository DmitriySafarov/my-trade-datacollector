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
        self._flush_lock = asyncio.Lock()
        self._timer_task: asyncio.Task[None] | None = None
        self._closed = False

    async def add(self, record: T) -> None:
        await self.add_many([record])

    async def add_many(self, records: list[T]) -> None:
        if self._closed:
            raise RuntimeError(f"BatchWriter {self.name} is closed")

        async with self._flush_lock:
            was_empty = not self._buffer
            self._buffer.extend(records)
            should_flush = len(self._buffer) >= self.count_limit
            if was_empty and not should_flush:
                self._schedule_timer()

        if should_flush:
            await self.flush()

    async def flush(self) -> None:
        async with self._flush_lock:
            if not self._buffer:
                return

            payload = self._buffer
            self._buffer = []
            self._cancel_timer()

        await self.flush_callback(payload)

    async def close(self) -> None:
        self._closed = True
        await self.flush()
        self._cancel_timer()

    def snapshot(self) -> dict[str, float | int | str]:
        return {
            "name": self.name,
            "buffer_size": len(self._buffer),
            "count_limit": self.count_limit,
            "time_limit_seconds": self.time_limit_seconds,
        }

    def _schedule_timer(self) -> None:
        self._cancel_timer()
        self._timer_task = asyncio.create_task(self._timer_flush())

    def _cancel_timer(self) -> None:
        if self._timer_task is None:
            return
        self._timer_task.cancel()
        self._timer_task = None

    async def _timer_flush(self) -> None:
        try:
            await asyncio.sleep(self.time_limit_seconds)
            await self.flush()
        except asyncio.CancelledError:
            return
