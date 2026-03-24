"""Base REST polling collector with error recovery and health tracking."""

from __future__ import annotations

import asyncio
import logging
from abc import abstractmethod
from typing import Any

import aiohttp

from src.collectors.base import BaseCollector

from ._poll_loop import compute_sleep_seconds, run_poll_loop
from .rate_limiter import SlidingWindowRateLimiter

LOGGER = logging.getLogger(__name__)

DEFAULT_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)


class BaseRestPoller(BaseCollector):
    """Abstract base for REST API polling collectors.

    Subclasses implement ``_poll(session)`` to execute one poll cycle.
    The base class runs the poll loop, handles error recovery with
    exponential backoff, tracks health, and implements the full
    ``BaseCollector`` lifecycle (start / stop / wait_ready / health_snapshot).
    """

    def __init__(
        self,
        *,
        name: str,
        source_ids: tuple[str, ...],
        interval_seconds: float,
        rate_limiter: SlidingWindowRateLimiter | None = None,
        timeout: aiohttp.ClientTimeout = DEFAULT_TIMEOUT,
        jitter_ratio: float = 0.1,
        max_startup_failures: int = 5,
        backoff_base_seconds: float = 5.0,
        backoff_max_seconds: float = 300.0,
    ) -> None:
        self.name = name
        self.source_ids = source_ids
        self._interval_seconds = interval_seconds
        self._rate_limiter = rate_limiter
        self._timeout = timeout
        self._jitter_ratio = jitter_ratio
        self._max_startup_failures = max_startup_failures
        self._backoff_base_seconds = backoff_base_seconds
        self._backoff_max_seconds = backoff_max_seconds

        # Safe in Python 3.10+: asyncio.Event() no longer binds to a
        # specific event loop at creation time.
        self._stop_event = asyncio.Event()
        self._ready_event = asyncio.Event()
        self._startup_error: BaseException | None = None
        self._consecutive_failures = 0
        self._last_success_at: float | None = None
        self._last_error_at: float | None = None
        self._last_error_message: str = ""
        self._total_polls = 0
        self._total_errors = 0

    # -- BaseCollector interface -------------------------------------------

    async def start(self) -> None:
        """Run the poll loop until stopped.

        **Blocking**: this coroutine runs indefinitely (until ``stop()``
        is called).  Callers must wrap it in ``asyncio.create_task()``
        to avoid blocking the event loop.
        """
        self._stop_event.clear()
        self._ready_event.clear()
        self._startup_error = None
        async with aiohttp.ClientSession(timeout=self._timeout) as session:
            await self._poll_loop(session)

    async def stop(self) -> None:
        self._stop_event.set()

    async def wait_ready(self) -> None:
        await self._ready_event.wait()
        if self._startup_error is not None:
            raise self._startup_error

    def health_snapshot(self) -> dict[str, object]:
        return {
            "name": self.name,
            "source_ids": list(self.source_ids),
            "interval_seconds": self._interval_seconds,
            "total_polls": self._total_polls,
            "total_errors": self._total_errors,
            "consecutive_failures": self._consecutive_failures,
            "last_success_at": self._last_success_at,
            "last_error_at": self._last_error_at,
            "last_error_message": self._last_error_message,
        }

    # -- Abstract hook for subclasses --------------------------------------

    @abstractmethod
    async def _poll(self, session: aiohttp.ClientSession) -> None:
        """Execute one poll cycle.  Raise on failure — the base class
        handles retry logic and health bookkeeping."""

    # -- Convenience helpers for subclass _poll implementations ------------

    async def _post_json(
        self,
        session: aiohttp.ClientSession,
        url: str,
        body: dict[str, Any],
        *,
        weight: int = 1,
    ) -> Any:
        """POST JSON with optional rate limiting and return parsed body."""
        if self._rate_limiter is not None:
            await self._rate_limiter.acquire(weight)
        async with session.post(url, json=body) as response:
            response.raise_for_status()
            return await response.json()

    async def _get_json(
        self,
        session: aiohttp.ClientSession,
        url: str,
        *,
        params: dict[str, str] | None = None,
        weight: int = 1,
    ) -> Any:
        """GET JSON with optional rate limiting and return parsed body."""
        if self._rate_limiter is not None:
            await self._rate_limiter.acquire(weight)
        async with session.get(url, params=params) as response:
            response.raise_for_status()
            return await response.json()

    # -- Internal poll loop (impl in _poll_loop.py) ------------------------

    async def _poll_loop(self, session: aiohttp.ClientSession) -> None:
        await run_poll_loop(self, session)

    def _compute_sleep_seconds(self) -> float:
        return compute_sleep_seconds(self)
