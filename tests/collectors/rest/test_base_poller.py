"""Tests for BaseRestPoller."""

from __future__ import annotations

import asyncio
from typing import Any

import aiohttp
import pytest

from src.collectors.rest.base_poller import BaseRestPoller
from src.collectors.rest.rate_limiter import SlidingWindowRateLimiter


# ------------------------------------------------------------------
# Concrete test poller
# ------------------------------------------------------------------


class _StubPoller(BaseRestPoller):
    """Minimal concrete implementation for testing the base class."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.poll_count = 0
        self.poll_error: Exception | None = None
        self.poll_barrier: asyncio.Event | None = None
        self.poll_entered: asyncio.Event | None = None

    async def _poll(self, session: aiohttp.ClientSession) -> None:
        if self.poll_entered is not None:
            self.poll_entered.set()
        if self.poll_barrier is not None:
            await self.poll_barrier.wait()
        if self.poll_error is not None:
            raise self.poll_error
        self.poll_count += 1


def _make_poller(**overrides: Any) -> _StubPoller:
    defaults: dict[str, Any] = {
        "name": "test_poller",
        "source_ids": ("test_source",),
        "interval_seconds": 0.05,
        "jitter_ratio": 0.0,
        "backoff_base_seconds": 0.02,
        "backoff_max_seconds": 0.1,
        "max_startup_failures": 3,
    }
    defaults.update(overrides)
    return _StubPoller(**defaults)


# ------------------------------------------------------------------
# Lifecycle tests
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_stop_lifecycle() -> None:
    poller = _make_poller()
    task = asyncio.create_task(poller.start())
    await poller.wait_ready()
    assert poller.poll_count >= 1
    await poller.stop()
    await asyncio.wait_for(task, timeout=2.0)
    assert poller.poll_count >= 1


@pytest.mark.asyncio
async def test_stop_during_sleep_exits_promptly() -> None:
    """Calling stop() while the poller sleeps between polls should exit fast."""
    poller = _make_poller(interval_seconds=60.0)
    task = asyncio.create_task(poller.start())
    await poller.wait_ready()
    # Poller is now sleeping for ~60s. Stop should wake it.
    await poller.stop()
    await asyncio.wait_for(task, timeout=2.0)
    assert poller.poll_count == 1


@pytest.mark.asyncio
async def test_wait_ready_resolves_after_first_success() -> None:
    barrier = asyncio.Event()
    poll_entered = asyncio.Event()
    poller = _make_poller()
    poller.poll_barrier = barrier
    poller.poll_entered = poll_entered

    task = asyncio.create_task(poller.start())
    # Deterministically wait until _poll is entered (no flaky sleep).
    await asyncio.wait_for(poll_entered.wait(), timeout=5.0)
    assert not poller._ready_event.is_set()

    barrier.set()
    await asyncio.wait_for(poller.wait_ready(), timeout=2.0)
    assert poller._ready_event.is_set()

    await poller.stop()
    await asyncio.wait_for(task, timeout=2.0)


# ------------------------------------------------------------------
# Polling and health tracking
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_multiple_polls_increment_counter() -> None:
    poller = _make_poller(interval_seconds=0.01)
    task = asyncio.create_task(poller.start())
    await asyncio.sleep(0.15)
    await poller.stop()
    await asyncio.wait_for(task, timeout=2.0)
    assert poller.poll_count >= 3


@pytest.mark.asyncio
async def test_health_snapshot_after_success() -> None:
    poller = _make_poller()
    task = asyncio.create_task(poller.start())
    await poller.wait_ready()
    snap = poller.health_snapshot()
    assert snap["name"] == "test_poller"
    assert snap["source_ids"] == ["test_source"]
    assert snap["total_polls"] >= 1
    assert snap["total_errors"] == 0
    assert snap["consecutive_failures"] == 0
    assert snap["last_success_at"] is not None
    assert snap["last_error_at"] is None
    await poller.stop()
    await asyncio.wait_for(task, timeout=2.0)


# ------------------------------------------------------------------
# Error handling and backoff
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_transient_error_retries_and_recovers() -> None:
    poller = _make_poller(max_startup_failures=10)
    call_count = 0

    original_poll = poller._poll

    async def failing_then_ok(session: aiohttp.ClientSession) -> None:
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            raise ConnectionError("transient")
        await original_poll(session)

    poller._poll = failing_then_ok  # type: ignore[assignment]

    task = asyncio.create_task(poller.start())
    await asyncio.wait_for(poller.wait_ready(), timeout=5.0)
    assert poller._total_errors == 2
    assert poller._consecutive_failures == 0  # reset after success
    await poller.stop()
    await asyncio.wait_for(task, timeout=2.0)


@pytest.mark.asyncio
async def test_startup_failure_raises_after_max_attempts() -> None:
    poller = _make_poller(max_startup_failures=3)
    poller.poll_error = ConnectionError("permanent")

    with pytest.raises(RuntimeError, match="failed 3 consecutive times"):
        await poller.start()
    assert poller._consecutive_failures == 3
    assert poller._total_errors == 3


@pytest.mark.asyncio
async def test_wait_ready_unblocks_and_raises_on_startup_failure() -> None:
    """wait_ready() must not deadlock when startup fails — it should raise."""
    poller = _make_poller(max_startup_failures=2, backoff_base_seconds=0.01)
    poller.poll_error = ConnectionError("permanent")

    task = asyncio.create_task(poller.start())
    with pytest.raises(RuntimeError, match="failed 2 consecutive times"):
        await asyncio.wait_for(poller.wait_ready(), timeout=5.0)
    # The start task should also have completed with the same error.
    with pytest.raises(RuntimeError, match="failed 2 consecutive times"):
        await task


@pytest.mark.asyncio
async def test_wait_ready_unblocks_on_task_cancellation() -> None:
    """wait_ready() must not deadlock if the start task is cancelled pre-ready."""
    barrier = asyncio.Event()
    poll_entered = asyncio.Event()
    poller = _make_poller(interval_seconds=60.0)
    poller.poll_barrier = barrier  # Block indefinitely in first poll
    poller.poll_entered = poll_entered  # Signals when _poll is entered

    task = asyncio.create_task(poller.start())
    # Wait until _poll is actually running (no flaky sleep).
    await asyncio.wait_for(poll_entered.wait(), timeout=5.0)

    # Cancel the start task while it's blocked in _poll (pre-ready).
    task.cancel()
    with pytest.raises(RuntimeError, match="terminated before becoming ready"):
        await asyncio.wait_for(poller.wait_ready(), timeout=5.0)
    # The start task should have been cancelled.
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_errors_after_ready_do_not_raise() -> None:
    """Once ready, errors are logged but don't kill the poller."""
    poller = _make_poller(interval_seconds=0.01, backoff_base_seconds=0.01)

    call_count = 0
    original_poll = poller._poll

    async def ok_then_fail(session: aiohttp.ClientSession) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            await original_poll(session)
            return
        raise ConnectionError("post-ready failure")

    poller._poll = ok_then_fail  # type: ignore[assignment]

    task = asyncio.create_task(poller.start())
    await poller.wait_ready()
    await asyncio.sleep(0.2)
    await poller.stop()
    await asyncio.wait_for(task, timeout=2.0)

    assert poller._total_polls == 1
    assert poller._total_errors >= 1
    snap = poller.health_snapshot()
    assert snap["last_error_message"] != ""


def test_backoff_increases_sleep_on_consecutive_failures() -> None:
    poller = _make_poller(
        backoff_base_seconds=0.1,
        backoff_max_seconds=10.0,
        max_startup_failures=100,
    )
    # 1st failure: backoff = 0.1 * 2^0 = 0.1
    # 2nd failure: backoff = 0.1 * 2^1 = 0.2
    poller._consecutive_failures = 1
    s1 = poller._compute_sleep_seconds()
    poller._consecutive_failures = 2
    s2 = poller._compute_sleep_seconds()
    poller._consecutive_failures = 3
    s3 = poller._compute_sleep_seconds()
    assert s2 > s1
    assert s3 > s2


def test_backoff_caps_at_max() -> None:
    poller = _make_poller(
        backoff_base_seconds=1.0,
        backoff_max_seconds=5.0,
    )
    poller._consecutive_failures = 100
    sleep = poller._compute_sleep_seconds()
    # Max backoff is 5.0 + up to 20% jitter = max 6.0
    assert sleep <= 6.5


def test_normal_sleep_has_jitter() -> None:
    poller = _make_poller(interval_seconds=10.0, jitter_ratio=0.1)
    sleeps = {poller._compute_sleep_seconds() for _ in range(20)}
    # With 10% jitter on 10s, we expect variation.
    assert len(sleeps) > 1


def test_normal_sleep_non_negative() -> None:
    poller = _make_poller(interval_seconds=0.5, jitter_ratio=0.0)
    assert poller._compute_sleep_seconds() >= 0.0


# ------------------------------------------------------------------
# Rate limiter integration
# ------------------------------------------------------------------


def test_rate_limiter_attached_at_construction() -> None:
    """Verify that rate_limiter kwarg is stored and accessible."""
    limiter = SlidingWindowRateLimiter(max_weight=1200, window_seconds=60)
    poller = _make_poller(rate_limiter=limiter)
    assert poller._rate_limiter is limiter
    assert limiter.available_weight() == 1200


def test_no_rate_limiter_by_default() -> None:
    poller = _make_poller()
    assert poller._rate_limiter is None
