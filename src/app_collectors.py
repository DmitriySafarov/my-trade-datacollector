from __future__ import annotations

import asyncio
import logging

from src.app_cleanup_state import CollectorStartupInterrupted, attach_cleanup_state
from src.app_lifecycle import CollectorShutdownError, CollectorStartupError
from src.collectors.base import BaseCollector


LOGGER = logging.getLogger(__name__)
TASK_DRAIN_TIMEOUT_SECONDS = 1.0


async def start_collectors(
    collectors: list[BaseCollector],
    tasks: dict[BaseCollector, asyncio.Task[None]],
    *,
    stop_event: asyncio.Event,
    startup_timeout: float,
) -> list[BaseCollector]:
    started: list[BaseCollector] = []
    for collector in collectors:
        if stop_event.is_set():
            await _stop_started_collectors(started, tasks)
            return []
        task = asyncio.create_task(
            collector.start(), name=f"collector:{collector.name}"
        )
        tasks[collector] = task
        try:
            await asyncio.wait_for(
                _wait_until_ready(collector, task, stop_event),
                timeout=startup_timeout,
            )
        except CollectorStartupInterrupted:
            await _stop_started_collectors([*started, collector], tasks)
            return []
        except asyncio.TimeoutError as error:
            cleanup_required = await _rollback_collectors([*started, collector], tasks)
            startup_error = CollectorStartupError(
                f"collector readiness timed out: {collector.name}"
            )
            attach_cleanup_state(startup_error, cleanup_required)
            raise startup_error from error
        except BaseException as error:
            cleanup_required = await _rollback_collectors([*started, collector], tasks)
            attach_cleanup_state(error, cleanup_required)
            raise
        started.append(collector)
    return started


async def stop_collectors(
    collectors: list[BaseCollector],
    tasks: dict[BaseCollector, asyncio.Task[None]],
) -> list[tuple[BaseCollector, BaseException]]:
    if not collectors:
        return []
    results = await asyncio.gather(
        *(collector.stop() for collector in collectors),
        return_exceptions=True,
    )
    failures: list[tuple[BaseCollector, BaseException]] = []
    for collector, result in zip(collectors, results, strict=True):
        if isinstance(result, BaseException):
            LOGGER.error(
                "collector_stop_failed collector=%s",
                collector.name,
                exc_info=result,
            )
            failures.append((collector, result))
        task_error = await _drain_collector_task(collector, tasks)
        if task_error is not None:
            failures.append((collector, task_error))
    return failures


async def _stop_started_collectors(
    collectors: list[BaseCollector],
    tasks: dict[BaseCollector, asyncio.Task[None]],
) -> None:
    failures = await stop_collectors(collectors, tasks)
    if failures:
        error = CollectorShutdownError(
            "collector shutdown failed for: "
            + ", ".join(collector.name for collector, _ in failures)
        )
        attach_cleanup_state(
            error,
            list(dict.fromkeys(collector for collector, _ in failures)),
        )
        raise error


async def _rollback_collectors(
    collectors: list[BaseCollector],
    tasks: dict[BaseCollector, asyncio.Task[None]],
) -> list[BaseCollector]:
    failures = await stop_collectors(collectors, tasks)
    for collector, error in failures:
        LOGGER.error(
            "collector_stop_after_start_failure_failed collector=%s",
            collector.name,
            exc_info=error,
        )
    return list(dict.fromkeys(collector for collector, _ in failures))


async def _wait_until_ready(
    collector: BaseCollector,
    task: asyncio.Task[None],
    stop_event: asyncio.Event,
) -> None:
    ready_task = asyncio.create_task(
        collector.wait_ready(), name=f"collector-ready:{collector.name}"
    )
    stop_task = asyncio.create_task(
        stop_event.wait(), name=f"collector-stop:{collector.name}"
    )
    try:
        while True:
            done, _ = await asyncio.wait(
                {task, ready_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if task in done:
                try:
                    await task
                except asyncio.CancelledError as error:
                    raise CollectorStartupError(
                        f"collector start cancelled before readiness: {collector.name}"
                    ) from error
                if stop_task.done() and stop_task.result():
                    raise CollectorStartupInterrupted
                await ready_task
                return
            if stop_task in done and stop_task.result():
                raise CollectorStartupInterrupted
            if ready_task in done:
                await ready_task
                return
    finally:
        for pending in (ready_task, stop_task):
            pending.cancel()
        await asyncio.gather(ready_task, stop_task, return_exceptions=True)


async def _drain_collector_task(
    collector: BaseCollector,
    tasks: dict[BaseCollector, asyncio.Task[None]],
) -> BaseException | None:
    task = tasks.pop(collector, None)
    if task is None:
        return None
    if not task.done():
        try:
            await asyncio.wait_for(
                asyncio.shield(task),
                timeout=TASK_DRAIN_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            task.cancel()
            try:
                await asyncio.wait_for(task, timeout=TASK_DRAIN_TIMEOUT_SECONDS)
            except asyncio.CancelledError:
                return None
            except asyncio.TimeoutError as error:
                LOGGER.error(
                    "collector_task_cancel_timeout collector=%s",
                    collector.name,
                    exc_info=error,
                )
                return RuntimeError(
                    f"collector task did not exit after cancellation: {collector.name}"
                )
            except Exception as error:
                LOGGER.error(
                    "collector_start_task_failed collector=%s",
                    collector.name,
                    exc_info=error,
                )
                return error
            return None
    try:
        await task
    except asyncio.CancelledError:
        return None
    except Exception as error:
        LOGGER.error(
            "collector_start_task_failed collector=%s",
            collector.name,
            exc_info=error,
        )
        return error
    return None
