from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Protocol

from src.db.batch_writer import BatchWriter

MANAGER_TASK_CANCEL_TIMEOUT_SECONDS = 6.0
WRITER_CLOSE_TIMEOUT_SECONDS = 6.0
WRITER_ABORT_TIMEOUT_SECONDS = 6.0


class SupportsAsyncStop(Protocol):
    async def stop(self) -> None: ...


def merge_error(
    primary: BaseException | None,
    secondary: BaseException | None,
) -> BaseException | None:
    if secondary is None:
        return primary
    if primary is None or secondary is primary:
        return secondary if primary is None else primary
    primary.add_note(f"Shutdown also failed: {secondary!r}")
    return primary


async def stop_manager_task(
    manager: SupportsAsyncStop,
    task: asyncio.Task[None],
) -> BaseException | None:
    error: BaseException | None = None
    stop_failed = False
    try:
        await manager.stop()
    except BaseException as caught:
        error = merge_error(error, caught)
        stop_failed = True
    try:
        if stop_failed and not task.done():
            task.cancel()
            await asyncio.wait_for(task, timeout=MANAGER_TASK_CANCEL_TIMEOUT_SECONDS)
        else:
            await task
    except asyncio.CancelledError as caught:
        if not stop_failed:
            error = merge_error(error, caught)
    except BaseException as caught:
        error = merge_error(error, caught)
    return error


async def collect_task_errors(
    tasks: Sequence[asyncio.Task[object]],
) -> BaseException | None:
    error: BaseException | None = None
    for task in tasks:
        try:
            await task
        except BaseException as caught:
            error = merge_error(error, caught)
    return error


async def close_writers(
    writers: Sequence[BatchWriter[tuple[object, ...]]],
    *,
    close_timeout_seconds: float | None = None,
    abort_timeout_seconds: float | None = None,
) -> BaseException | None:
    error: BaseException | None = None
    close_timeout = (
        WRITER_CLOSE_TIMEOUT_SECONDS
        if close_timeout_seconds is None
        else close_timeout_seconds
    )
    abort_timeout = (
        WRITER_ABORT_TIMEOUT_SECONDS
        if abort_timeout_seconds is None
        else abort_timeout_seconds
    )
    for writer in writers:
        try:
            await asyncio.wait_for(writer.close(), timeout=close_timeout)
        except TimeoutError as caught:
            timeout_error = RuntimeError(
                f"BatchWriter {writer.name} close timed out after {close_timeout:.2f}s"
            )
            timeout_error.__cause__ = caught
            try:
                await asyncio.wait_for(writer.abort(), timeout=abort_timeout)
            except TimeoutError as abort_caught:
                abort_error = RuntimeError(
                    f"BatchWriter {writer.name} abort timed out after {abort_timeout:.2f}s"
                )
                abort_error.__cause__ = abort_caught
                timeout_error = merge_error(timeout_error, abort_error) or timeout_error
            except BaseException as abort_caught:
                timeout_error = (
                    merge_error(timeout_error, abort_caught) or timeout_error
                )
            error = merge_error(error, timeout_error)
        except BaseException as caught:
            error = merge_error(error, caught)
    return error
