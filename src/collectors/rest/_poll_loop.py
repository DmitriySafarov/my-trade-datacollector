"""Internal poll loop with error recovery for BaseRestPoller.

Extracted from base_poller.py to keep the public module under 200 lines.
This module is an implementation detail — import from base_poller instead.
"""

from __future__ import annotations

import asyncio
import logging
import random
from time import monotonic
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import aiohttp

    from .base_poller import BaseRestPoller

LOGGER = logging.getLogger(__name__)


async def run_poll_loop(poller: BaseRestPoller, session: aiohttp.ClientSession) -> None:
    """Execute the poll loop with error recovery and startup signalling.

    Three paths set ``_ready_event`` to prevent ``wait_ready()`` deadlocks:

    1. Normal — first successful poll sets ``_ready_event``.
    2. Startup failure — ``max_startup_failures`` reached, stores error
       and sets event before re-raising.
    3. Fatal — ``CancelledError`` / ``KeyboardInterrupt``, stores a
       descriptive error and sets event before re-raising.
    """
    ready_signalled = False
    try:
        while not poller._stop_event.is_set():
            try:
                await poller._poll(session)
                poller._total_polls += 1
                poller._consecutive_failures = 0
                poller._last_success_at = monotonic()
                if not ready_signalled:
                    poller._ready_event.set()
                    ready_signalled = True
            except Exception as error:
                poller._total_errors += 1
                poller._consecutive_failures += 1
                poller._last_error_at = monotonic()
                poller._last_error_message = repr(error)
                LOGGER.warning(
                    "rest_poll_failed source=%s consecutive=%d error=%r",
                    poller.name,
                    poller._consecutive_failures,
                    error,
                )
                if (
                    not ready_signalled
                    and poller._consecutive_failures >= poller._max_startup_failures
                ):
                    startup_error = RuntimeError(
                        f"REST poller {poller.name} failed "
                        f"{poller._consecutive_failures} consecutive times "
                        f"during startup"
                    )
                    startup_error.__cause__ = error
                    poller._startup_error = startup_error
                    poller._ready_event.set()
                    raise startup_error
            sleep_seconds = compute_sleep_seconds(poller)
            try:
                await asyncio.wait_for(
                    poller._stop_event.wait(),
                    timeout=sleep_seconds,
                )
                return  # stop was requested
            except asyncio.TimeoutError:
                continue  # normal: sleep elapsed, loop again
    except BaseException:
        if not ready_signalled:
            # Unblock wait_ready() on CancelledError or other fatal errors
            # so callers don't deadlock.
            if poller._startup_error is None:
                poller._startup_error = RuntimeError(
                    f"REST poller {poller.name} terminated before becoming ready"
                )
            poller._ready_event.set()
        raise


def compute_sleep_seconds(poller: BaseRestPoller) -> float:
    """Compute next sleep duration with exponential backoff on failure."""
    if poller._consecutive_failures > 0:
        backoff = min(
            poller._backoff_base_seconds * (2 ** (poller._consecutive_failures - 1)),
            poller._backoff_max_seconds,
        )
        jitter = random.uniform(0, backoff * 0.2)
        return backoff + jitter
    base = poller._interval_seconds
    jitter = random.uniform(-base * poller._jitter_ratio, base * poller._jitter_ratio)
    return max(0.0, base + jitter)
