"""Shared base for Binance Futures WebSocket collectors.

Extracts the identical lifecycle logic (start, stop, wait_ready,
health_snapshot) used by all Binance WS collectors that follow the
``BinanceWsManager + BatchWriter`` pattern.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from src.collectors.base import BaseCollector
from src.db.batch_writer import BatchWriter

from .ws_manager import BinanceWsManager
from .ws_types import StreamConfig

LOGGER = logging.getLogger(__name__)


class BinanceWsCollectorBase(BaseCollector):
    """Base class for Binance WS collectors using BinanceWsManager + BatchWriter."""

    # Subclasses MUST override these class attributes.
    name: str = ""
    source_ids: tuple[str, ...] = ()

    def __init__(
        self,
        *,
        writer: BatchWriter[tuple[object, ...]],
        streams: list[StreamConfig],
        base_ws_url: str = "wss://fstream.binance.com/stream",
        reconnect_base_seconds: float = 1.0,
        reconnect_max_seconds: float = 30.0,
        reconnect_jitter_ratio: float = 0.2,
    ) -> None:
        self._writer = writer
        self._manager = BinanceWsManager(
            streams=streams,
            base_ws_url=base_ws_url,
            reconnect_base_seconds=reconnect_base_seconds,
            reconnect_max_seconds=reconnect_max_seconds,
            reconnect_jitter_ratio=reconnect_jitter_ratio,
        )
        self._stopped = asyncio.Event()
        self._stopped.set()

    async def start(self) -> None:
        self._stopped.clear()
        manager_task = asyncio.create_task(self._manager.run())
        writer_fail_task = asyncio.create_task(self._writer.wait_failure())
        error: BaseException | None = None
        try:
            done, _ = await asyncio.wait(
                {manager_task, writer_fail_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if writer_fail_task in done:
                error = _extract_error(writer_fail_task)
                await _stop_manager(self._manager, manager_task)
            else:
                error = _extract_error(manager_task)
        except BaseException as caught:
            error = caught
        finally:
            writer_fail_task.cancel()
            await asyncio.gather(writer_fail_task, return_exceptions=True)
            if not manager_task.done():
                await _stop_manager(self._manager, manager_task)
            try:
                await self._writer.close()
            except Exception as close_err:
                error = error or close_err
            self._stopped.set()
        if error is not None:
            raise error

    async def stop(self) -> None:
        try:
            await self._manager.stop()
        finally:
            await self._stopped.wait()

    async def wait_ready(self) -> None:
        await self._manager.wait_ready()

    def health_snapshot(self) -> dict[str, Any]:
        snapshot = self._manager.health_snapshot()
        snapshot["name"] = self.name
        snapshot["source_ids"] = list(self.source_ids)
        snapshot["writer"] = self._writer.snapshot()
        return snapshot


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _stop_manager(
    manager: BinanceWsManager,
    task: asyncio.Task[None],
) -> None:
    """Stop the WS manager and wait for its task, swallowing errors."""
    try:
        await manager.stop()
    except Exception:
        pass
    try:
        await asyncio.wait_for(task, timeout=5.0)
    except Exception:
        pass


def _extract_error(task: asyncio.Task[None]) -> BaseException | None:
    """Extract exception from a done task, if any."""
    if task.cancelled():
        return asyncio.CancelledError()
    return task.exception()
