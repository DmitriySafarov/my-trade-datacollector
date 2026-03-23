from __future__ import annotations

import asyncio

import pytest

from src.collectors.hyperliquid.lifecycle_support import (
    close_writers,
    stop_manager_task,
)
from src.db.batch_writer import BatchWriter


class _FailingStopManager:
    async def stop(self) -> None:
        raise RuntimeError("stop boom")


@pytest.mark.asyncio
async def test_stop_manager_task_cancels_hung_task_when_stop_fails() -> None:
    blocker = asyncio.Event()

    async def run_forever() -> None:
        await blocker.wait()

    task = asyncio.create_task(run_forever())

    error = await stop_manager_task(_FailingStopManager(), task)

    assert isinstance(error, RuntimeError)
    assert str(error) == "stop boom"
    assert task.cancelled() is True


@pytest.mark.asyncio
async def test_close_writers_aborts_hung_flush_after_timeout() -> None:
    cancelled = asyncio.Event()

    async def hanging_flush(payload: list[str]) -> None:
        del payload
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    writer = BatchWriter(
        name="hung-close",
        count_limit=10,
        time_limit_seconds=60.0,
        flush_callback=hanging_flush,
    )
    await writer.add("trade-1")

    error = await close_writers([writer], close_timeout_seconds=0.01)

    assert isinstance(error, RuntimeError)
    assert str(error) == "BatchWriter hung-close close timed out after 0.01s"
    await asyncio.wait_for(cancelled.wait(), timeout=1.0)


@pytest.mark.asyncio
async def test_close_writers_bounds_abort_and_continues_to_next_writer() -> None:
    cancel_seen = asyncio.Event()
    flushed: list[list[str]] = []

    async def stubborn_flush(payload: list[str]) -> None:
        del payload
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancel_seen.set()
            await asyncio.sleep(60.0)

    async def quick_flush(payload: list[str]) -> None:
        flushed.append(payload)

    stuck_writer = BatchWriter(
        name="hung-abort",
        count_limit=10,
        time_limit_seconds=60.0,
        flush_callback=stubborn_flush,
    )
    quick_writer = BatchWriter(
        name="quick-close",
        count_limit=10,
        time_limit_seconds=60.0,
        flush_callback=quick_flush,
    )
    await stuck_writer.add("trade-1")
    await quick_writer.add("trade-2")

    error = await close_writers(
        [stuck_writer, quick_writer],
        close_timeout_seconds=0.01,
        abort_timeout_seconds=0.01,
    )

    assert isinstance(error, RuntimeError)
    assert str(error) == "BatchWriter hung-abort close timed out after 0.01s"
    assert any(
        "BatchWriter hung-abort abort timed out after 0.01s" in note
        for note in error.__notes__ or []
    )
    assert flushed == [["trade-2"]]
    await asyncio.wait_for(cancel_seen.wait(), timeout=1.0)
