from __future__ import annotations

import asyncio

import pytest

from src.db.batch_writer import BatchWriter


@pytest.mark.asyncio
async def test_timer_flush_does_not_cancel_its_own_callback() -> None:
    flushed = asyncio.Event()
    payloads: list[list[str]] = []

    async def flush_callback(payload: list[str]) -> None:
        payloads.append(payload)
        flushed.set()

    writer = BatchWriter(
        name="timer",
        count_limit=10,
        time_limit_seconds=0.01,
        flush_callback=flush_callback,
    )
    await writer.add("trade-1")

    await asyncio.wait_for(flushed.wait(), timeout=1.0)

    assert payloads == [["trade-1"]]
    assert writer.snapshot()["buffer_size"] == 0
    await writer.close()


@pytest.mark.asyncio
async def test_timer_flush_failure_requeues_buffer_for_close_retry() -> None:
    attempts: list[list[str]] = []

    async def flush_callback(payload: list[str]) -> None:
        attempts.append(payload)
        if len(attempts) == 1:
            raise RuntimeError("flush failed")

    writer = BatchWriter(
        name="retry",
        count_limit=10,
        time_limit_seconds=0.01,
        flush_callback=flush_callback,
    )
    await writer.add("trade-1")

    with pytest.raises(RuntimeError, match="flush failed"):
        await asyncio.wait_for(writer.wait_failure(), timeout=1.0)

    assert writer.snapshot()["buffer_size"] == 1

    await writer.close()

    assert attempts == [["trade-1"], ["trade-1"]]


@pytest.mark.asyncio
async def test_flush_callbacks_are_serialized_in_arrival_order() -> None:
    started = asyncio.Event()
    release_first = asyncio.Event()
    call_order: list[tuple[str, list[str]]] = []

    async def flush_callback(payload: list[str]) -> None:
        call_order.append(("start", payload))
        if payload == ["trade-1"]:
            started.set()
            await release_first.wait()
        call_order.append(("finish", payload))

    writer = BatchWriter(
        name="serial",
        count_limit=1,
        time_limit_seconds=60.0,
        flush_callback=flush_callback,
    )
    first = asyncio.create_task(writer.add("trade-1"))

    await asyncio.wait_for(started.wait(), timeout=1.0)
    second = asyncio.create_task(writer.add("trade-2"))
    await asyncio.sleep(0.01)

    assert call_order == [("start", ["trade-1"])]

    release_first.set()
    await asyncio.gather(first, second)

    assert call_order == [
        ("start", ["trade-1"]),
        ("finish", ["trade-1"]),
        ("start", ["trade-2"]),
        ("finish", ["trade-2"]),
    ]


@pytest.mark.asyncio
async def test_cancelling_waiter_does_not_cancel_shared_flush_task() -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    finished = asyncio.Event()
    payloads: list[list[str]] = []

    async def flush_callback(payload: list[str]) -> None:
        payloads.append(payload)
        started.set()
        await release.wait()
        finished.set()

    writer = BatchWriter(
        name="shielded",
        count_limit=1,
        time_limit_seconds=60.0,
        flush_callback=flush_callback,
    )
    add_task = asyncio.create_task(writer.add("trade-1"))

    await asyncio.wait_for(started.wait(), timeout=1.0)
    add_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await add_task

    release.set()
    await asyncio.wait_for(finished.wait(), timeout=1.0)
    await writer.close()

    assert payloads == [["trade-1"]]
    assert writer.failure is None
