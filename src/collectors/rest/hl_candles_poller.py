"""Hyperliquid REST candleSnapshot poller.

Polls the Hyperliquid ``/info`` endpoint every 4 hours for each
coin × interval combination, writing closed candle snapshots to
``hl_rest_candles`` for gap-fill and verification against WS data.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

import aiohttp
import asyncpg

from src.collectors.hyperliquid.candle_parsing import VALID_INTERVALS

from .base_poller import BaseRestPoller
from .hl_candles_parsing import SOURCE_ID, RestCandleRecord, parse_rest_candle
from .hl_candles_storage import RestCandleStore
from .rate_limiter import SlidingWindowRateLimiter

LOGGER = logging.getLogger(__name__)

DEFAULT_COINS: tuple[str, ...] = ("ETH", "BTC")
DEFAULT_INTERVALS: tuple[str, ...] = VALID_INTERVALS
DEFAULT_POLL_SECONDS: float = 14_400.0  # 4 hours
DEFAULT_LOOKBACK_SECONDS: float = 86_400.0  # 24 hours
# Hyperliquid candleSnapshot is a lightweight read; weight=1 per call.
API_WEIGHT: int = 1


class HyperliquidCandlesPoller(BaseRestPoller):
    """Poll Hyperliquid REST candleSnapshot for gap-fill.

    On each poll cycle iterates ``coins × intervals``, fetching the
    last ``lookback_seconds`` of closed candles.  Duplicates are
    handled at the DB level via ON CONFLICT DO NOTHING on the
    natural key ``(source, time, coin, interval)``.
    """

    def __init__(
        self,
        *,
        base_url: str,
        pool: asyncpg.Pool,
        coins: Sequence[str] = DEFAULT_COINS,
        intervals: Sequence[str] = DEFAULT_INTERVALS,
        interval_seconds: float = DEFAULT_POLL_SECONDS,
        lookback_seconds: float = DEFAULT_LOOKBACK_SECONDS,
        rate_limiter: SlidingWindowRateLimiter | None = None,
        **kwargs: object,
    ) -> None:
        super().__init__(
            name="hl_rest_candles",
            source_ids=(SOURCE_ID,),
            interval_seconds=interval_seconds,
            rate_limiter=rate_limiter,
            **kwargs,
        )
        self._api_url = f"{base_url.rstrip('/')}/info"
        self._store = RestCandleStore(pool)
        self._coins = tuple(c.upper() for c in coins)
        self._intervals = tuple(intervals)
        self._lookback_seconds = lookback_seconds
        self._total_parse_errors = 0

    async def _poll(self, session: aiohttp.ClientSession) -> None:
        """Execute one poll cycle: fetch candles for all coins × intervals."""
        now = datetime.now(UTC)
        start_ms = int(
            (now - timedelta(seconds=self._lookback_seconds)).timestamp() * 1000
        )
        end_ms = int(now.timestamp() * 1000)

        total_fetched = 0
        total_skipped = 0
        total_parse_errors = 0
        total_errors = 0

        for coin in self._coins:
            for interval in self._intervals:
                try:
                    records, skipped, parse_errors = await self._fetch_interval(
                        session,
                        coin,
                        interval,
                        start_ms,
                        end_ms,
                    )
                except Exception:
                    LOGGER.warning(
                        "hl_rest_candles_fetch_failed coin=%s interval=%s",
                        coin,
                        interval,
                        exc_info=True,
                    )
                    total_errors += 1
                    continue
                if records:
                    await self._store.write_many([r.as_row() for r in records])
                    total_fetched += len(records)
                total_skipped += skipped
                total_parse_errors += parse_errors

        self._total_parse_errors += total_parse_errors
        total_expected = len(self._coins) * len(self._intervals)
        if total_errors == 0:
            LOGGER.info(
                "hl_rest_candles_poll_complete fetched=%d skipped_open=%d"
                " parse_errors=%d",
                total_fetched,
                total_skipped,
                total_parse_errors,
            )
        elif total_errors == total_expected:
            LOGGER.warning(
                "hl_rest_candles_poll_failed all %d fetches failed",
                total_errors,
            )
            raise RuntimeError(f"all {total_errors} coin×interval fetches failed")
        elif total_errors * 2 >= total_expected:
            LOGGER.warning(
                "hl_rest_candles_poll_degraded fetched=%d errors=%d total=%d",
                total_fetched,
                total_errors,
                total_expected,
            )
            raise RuntimeError(
                f"{total_errors}/{total_expected} coin×interval fetches failed "
                f"(>=50% threshold)"
            )
        else:
            LOGGER.warning(
                "hl_rest_candles_poll_partial_errors fetched=%d errors=%d"
                " parse_errors=%d total=%d",
                total_fetched,
                total_errors,
                total_parse_errors,
                total_expected,
            )

    def health_snapshot(self) -> dict[str, object]:
        snapshot = super().health_snapshot()
        snapshot["total_parse_errors"] = self._total_parse_errors
        return snapshot

    async def _fetch_interval(
        self,
        session: aiohttp.ClientSession,
        coin: str,
        interval: str,
        start_ms: int,
        end_ms: int,
    ) -> tuple[list[RestCandleRecord], int, int]:
        """Fetch candle snapshots for a single coin × interval window.

        Returns ``(records, skipped_open, parse_errors)`` — closed candle
        records, count of silently skipped open candles, and count of
        candles that failed parsing.
        """
        body = {
            "type": "candleSnapshot",
            "req": {
                "coin": coin,
                "interval": interval,
                "startTime": start_ms,
                "endTime": end_ms,
            },
        }
        data = await self._post_json(session, self._api_url, body, weight=API_WEIGHT)
        if not isinstance(data, list):
            LOGGER.warning(
                "hl_rest_candles_unexpected_response coin=%s interval=%s type=%s",
                coin,
                interval,
                type(data).__name__,
            )
            return [], 0, 0
        records: list[RestCandleRecord] = []
        skipped_open = 0
        parse_errors = 0
        for index, candle in enumerate(data):
            try:
                record = parse_rest_candle(candle, allowed_coins=self._coins)
                if record is None:
                    skipped_open += 1
                else:
                    records.append(record)
            except Exception as error:
                parse_errors += 1
                LOGGER.warning(
                    "hl_rest_candle_parse_error coin=%s interval=%s idx=%d error=%r",
                    coin,
                    interval,
                    index,
                    error,
                )
        return records, skipped_open, parse_errors
