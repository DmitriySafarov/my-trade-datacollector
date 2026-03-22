from __future__ import annotations

import asyncio
from contextlib import suppress

from src.app_lifecycle import CollectorShutdownError
from src.collectors.base import BaseCollector


async def run_until_stopped(
    stop_event: asyncio.Event,
    collector_tasks: dict[BaseCollector, asyncio.Task[None]],
) -> None:
    if not collector_tasks:
        await stop_event.wait()
        return
    stop_task = asyncio.create_task(stop_event.wait(), name="collector-stop")
    try:
        done, _ = await asyncio.wait(
            {stop_task, *collector_tasks.values()},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if stop_task in done:
            return
        completed = next(task for task in done if task is not stop_task)
        collector = next(
            item for item, task in collector_tasks.items() if task is completed
        )
        stop_event.set()
        try:
            await completed
        except asyncio.CancelledError as error:
            raise CollectorShutdownError(
                f"collector task stopped unexpectedly: {collector.name}"
            ) from error
        except Exception as error:
            raise CollectorShutdownError(
                f"collector task failed: {collector.name}"
            ) from error
        raise CollectorShutdownError(
            f"collector task exited unexpectedly: {collector.name}"
        )
    finally:
        stop_task.cancel()
        with suppress(asyncio.CancelledError):
            await stop_task
