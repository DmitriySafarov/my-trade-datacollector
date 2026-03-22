from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from src.app import CollectorApplication


LOGGER = logging.getLogger(__name__)


class ApplicationStopTimeoutError(RuntimeError):
    pass


class CollectorStartupError(RuntimeError):
    pass


class CollectorShutdownError(RuntimeError):
    pass


async def stop_application(app: CollectorApplication) -> None:
    stop_task = asyncio.create_task(app.stop())
    timeout = app.config.shutdown_timeout_seconds
    try:
        await asyncio.wait_for(asyncio.shield(stop_task), timeout=timeout)
    except asyncio.TimeoutError:
        LOGGER.error("collector_stop_timeout timeout_seconds=%s", timeout)
        try:
            await asyncio.wait_for(asyncio.shield(stop_task), timeout=timeout)
        except asyncio.TimeoutError as error:
            LOGGER.critical("collector_stop_grace_timeout timeout_seconds=%s", timeout)
            stop_task.cancel()
            try:
                await asyncio.wait_for(stop_task, timeout=timeout)
            except asyncio.TimeoutError:
                LOGGER.critical(
                    "collector_stop_cancel_timeout timeout_seconds=%s", timeout
                )
            except asyncio.CancelledError:
                pass
            raise ApplicationStopTimeoutError(
                f"collector stop exceeded {timeout}s timeout and grace window"
            ) from error
