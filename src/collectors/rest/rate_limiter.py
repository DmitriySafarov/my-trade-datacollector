"""Sliding-window rate limiter for REST API weight budgets."""

from __future__ import annotations

import asyncio
from collections import deque
from time import monotonic


class SlidingWindowRateLimiter:
    """Track API weight usage over a sliding time window.

    Callers ``await acquire(weight)`` before each request.
    If the budget is exhausted the coroutine sleeps until enough
    weight expires from the window.

    Thread-safety is provided by an ``asyncio.Lock`` so the limiter
    can be shared across multiple pollers that hit the same API.
    """

    def __init__(
        self,
        *,
        max_weight: int,
        window_seconds: float = 60.0,
    ) -> None:
        if max_weight <= 0:
            raise ValueError("max_weight must be positive")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        self._max_weight = max_weight
        self._window_seconds = window_seconds
        self._entries: deque[tuple[float, int]] = deque()
        self._current_weight = 0
        self._lock = asyncio.Lock()

    @property
    def max_weight(self) -> int:
        return self._max_weight

    @property
    def window_seconds(self) -> float:
        return self._window_seconds

    async def acquire(self, weight: int = 1) -> None:
        """Wait until *weight* is available, then consume it."""
        if weight <= 0:
            raise ValueError("weight must be positive")
        if weight > self._max_weight:
            raise ValueError(
                f"requested weight {weight} exceeds max {self._max_weight}"
            )
        while True:
            async with self._lock:
                self._purge_expired()
                if self._current_weight + weight <= self._max_weight:
                    now = monotonic()
                    self._entries.append((now, weight))
                    self._current_weight += weight
                    return
                # Compute wait until enough weight expires.
                wait = self._wait_until_available(weight)
            await asyncio.sleep(max(0.01, wait))

    def available_weight(self) -> int:
        """Return currently available weight (non-blocking snapshot).

        **IMPORTANT**: Do not use this value to gate ``acquire()`` calls or
        make budget decisions.  The result may undercount available weight
        because expired entries are not purged without holding the lock.
        Only ``acquire()`` guarantees an accurate, consistent view.

        This method is intended for diagnostics and health reporting only.
        """
        return max(0, self._max_weight - self._current_weight)

    def snapshot(self) -> dict[str, object]:
        """Return a point-in-time snapshot without mutating internal state.

        Note: ``current_weight`` and ``pending_entries`` are read without
        holding the lock, so they may be mutually inconsistent if a
        concurrent ``acquire()`` is mid-flight on another task.  This is
        acceptable for health/diagnostics — use ``acquire()`` for accurate
        budget decisions.
        """
        return {
            "max_weight": self._max_weight,
            "current_weight": self._current_weight,
            "available_weight": max(0, self._max_weight - self._current_weight),
            "window_seconds": self._window_seconds,
            "pending_entries": len(self._entries),
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _purge_expired(self) -> None:
        cutoff = monotonic() - self._window_seconds
        while self._entries and self._entries[0][0] < cutoff:
            _, weight = self._entries.popleft()
            self._current_weight -= weight

    def _wait_until_available(self, needed: int) -> float:
        """Estimate how long to sleep until *needed* weight frees up."""
        now = monotonic()
        freed = 0
        for entry_time, entry_weight in self._entries:
            freed += entry_weight
            if self._current_weight - freed + needed <= self._max_weight:
                return max(0.0, entry_time + self._window_seconds - now)
        # Fallback: wait for the full window to elapse.
        return self._window_seconds
