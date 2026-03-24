"""Tests for SlidingWindowRateLimiter."""

from __future__ import annotations

import asyncio
from time import monotonic

import pytest

from src.collectors.rest.rate_limiter import SlidingWindowRateLimiter


def test_init_rejects_non_positive_max_weight() -> None:
    with pytest.raises(ValueError, match="max_weight must be positive"):
        SlidingWindowRateLimiter(max_weight=0, window_seconds=60)


def test_init_rejects_non_positive_window() -> None:
    with pytest.raises(ValueError, match="window_seconds must be positive"):
        SlidingWindowRateLimiter(max_weight=100, window_seconds=0)


@pytest.mark.asyncio
async def test_acquire_rejects_zero_weight() -> None:
    limiter = SlidingWindowRateLimiter(max_weight=100, window_seconds=60)
    with pytest.raises(ValueError, match="weight must be positive"):
        await limiter.acquire(0)


@pytest.mark.asyncio
async def test_acquire_rejects_weight_exceeding_max() -> None:
    limiter = SlidingWindowRateLimiter(max_weight=50, window_seconds=60)
    with pytest.raises(ValueError, match="exceeds max"):
        await limiter.acquire(51)


@pytest.mark.asyncio
async def test_acquire_consumes_weight() -> None:
    limiter = SlidingWindowRateLimiter(max_weight=100, window_seconds=60)
    assert limiter.available_weight() == 100
    await limiter.acquire(20)
    assert limiter.available_weight() == 80
    await limiter.acquire(30)
    assert limiter.available_weight() == 50


@pytest.mark.asyncio
async def test_acquire_up_to_exact_max() -> None:
    limiter = SlidingWindowRateLimiter(max_weight=100, window_seconds=60)
    await limiter.acquire(100)
    assert limiter.available_weight() == 0


@pytest.mark.asyncio
async def test_expired_entries_free_weight() -> None:
    limiter = SlidingWindowRateLimiter(max_weight=100, window_seconds=0.1)
    await limiter.acquire(100)
    assert limiter.available_weight() == 0
    await asyncio.sleep(0.15)
    # Trigger purge via acquire — read methods no longer mutate state.
    await limiter.acquire(1)
    assert limiter.available_weight() == 99


@pytest.mark.asyncio
async def test_acquire_blocks_until_weight_frees() -> None:
    limiter = SlidingWindowRateLimiter(max_weight=50, window_seconds=0.1)
    await limiter.acquire(50)

    start = monotonic()
    await limiter.acquire(10)
    elapsed = monotonic() - start
    assert elapsed >= 0.05  # should have waited for expiration


@pytest.mark.asyncio
async def test_concurrent_acquires_respect_budget() -> None:
    limiter = SlidingWindowRateLimiter(max_weight=30, window_seconds=60)

    async def grab(w: int) -> None:
        await limiter.acquire(w)

    await asyncio.gather(grab(10), grab(10), grab(10))
    assert limiter.available_weight() == 0


@pytest.mark.asyncio
async def test_snapshot_reflects_current_state() -> None:
    limiter = SlidingWindowRateLimiter(max_weight=1200, window_seconds=60)
    await limiter.acquire(20)
    snap = limiter.snapshot()
    assert snap["max_weight"] == 1200
    assert snap["current_weight"] == 20
    assert snap["available_weight"] == 1180
    assert snap["window_seconds"] == 60
    assert snap["pending_entries"] == 1


@pytest.mark.asyncio
async def test_properties_expose_config() -> None:
    limiter = SlidingWindowRateLimiter(max_weight=500, window_seconds=30)
    assert limiter.max_weight == 500
    assert limiter.window_seconds == 30


@pytest.mark.asyncio
async def test_partial_expiration_frees_only_expired_weight() -> None:
    """Two acquisitions at different times; only the older one expires."""
    limiter = SlidingWindowRateLimiter(max_weight=100, window_seconds=0.5)
    await limiter.acquire(30)
    await asyncio.sleep(0.3)
    await limiter.acquire(20)
    await asyncio.sleep(0.3)
    # First entry (30) expired; trigger purge via acquire.
    await limiter.acquire(1)
    # 30 expired, 20+1 active → 79 available
    assert limiter.available_weight() == 79
