"""Hyperliquid REST fundingHistory poller.

Polls the Hyperliquid ``/info`` endpoint every 8 hours for each
coin, writing historical funding rate records to ``hl_rest_funding``
for gap-fill and verification against WS activeAssetCtx data.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

import aiohttp
import asyncpg

from .base_poller import BaseRestPoller
from .hl_funding_parsing import SOURCE_ID, RestFundingRecord, parse_rest_funding
from .hl_funding_storage import RestFundingStore
from .rate_limiter import SlidingWindowRateLimiter

LOGGER = logging.getLogger(__name__)

DEFAULT_COINS: tuple[str, ...] = ("ETH", "BTC")
DEFAULT_POLL_SECONDS: float = 28_800.0  # 8 hours
DEFAULT_LOOKBACK_SECONDS: float = 172_800.0  # 48 hours
API_WEIGHT: int = 20


class HyperliquidFundingPoller(BaseRestPoller):
    """Poll Hyperliquid REST fundingHistory for gap-fill.

    On each poll cycle iterates ``coins``, fetching the last
    ``lookback_seconds`` of funding rate records.  Duplicates are
    handled at the DB level via ON CONFLICT DO NOTHING on the
    natural key ``(source, time, coin)``.
    """

    def __init__(
        self,
        *,
        base_url: str,
        pool: asyncpg.Pool,
        coins: Sequence[str] = DEFAULT_COINS,
        interval_seconds: float = DEFAULT_POLL_SECONDS,
        lookback_seconds: float = DEFAULT_LOOKBACK_SECONDS,
        rate_limiter: SlidingWindowRateLimiter | None = None,
        **kwargs: object,
    ) -> None:
        super().__init__(
            name="hl_rest_funding",
            source_ids=(SOURCE_ID,),
            interval_seconds=interval_seconds,
            rate_limiter=rate_limiter,
            **kwargs,
        )
        self._api_url = f"{base_url.rstrip('/')}/info"
        self._store = RestFundingStore(pool)
        self._coins = tuple(c.upper() for c in coins)
        self._lookback_seconds = lookback_seconds
        self._total_parse_errors = 0

    async def _poll(self, session: aiohttp.ClientSession) -> None:
        """Execute one poll cycle: fetch funding history for all coins."""
        now = datetime.now(UTC)
        start_ms = int(
            (now - timedelta(seconds=self._lookback_seconds)).timestamp() * 1000
        )
        end_ms = int(now.timestamp() * 1000)

        total_fetched = 0
        total_parse_errors = 0
        total_errors = 0

        for coin in self._coins:
            try:
                records, parse_errors = await self._fetch_coin(
                    session, coin, start_ms, end_ms
                )
            except Exception:
                LOGGER.warning(
                    "hl_rest_funding_fetch_failed coin=%s",
                    coin,
                    exc_info=True,
                )
                total_errors += 1
                continue
            if records:
                await self._store.write_many([r.as_row() for r in records])
                total_fetched += len(records)
            total_parse_errors += parse_errors

        self._total_parse_errors += total_parse_errors
        total_expected = len(self._coins)
        if total_errors == 0:
            LOGGER.info(
                "hl_rest_funding_poll_complete fetched=%d parse_errors=%d",
                total_fetched,
                total_parse_errors,
            )
        elif total_errors == total_expected:
            LOGGER.warning(
                "hl_rest_funding_poll_failed all %d fetches failed",
                total_errors,
            )
            raise RuntimeError(f"all {total_errors} coin fetches failed")
        elif total_errors * 2 >= total_expected:
            LOGGER.warning(
                "hl_rest_funding_poll_degraded fetched=%d errors=%d total=%d",
                total_fetched,
                total_errors,
                total_expected,
            )
            raise RuntimeError(
                f"{total_errors}/{total_expected} coin fetches failed (>=50% threshold)"
            )
        else:
            LOGGER.warning(
                "hl_rest_funding_poll_partial_errors fetched=%d errors=%d"
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

    async def _fetch_coin(
        self,
        session: aiohttp.ClientSession,
        coin: str,
        start_ms: int,
        end_ms: int,
    ) -> tuple[list[RestFundingRecord], int]:
        """Fetch funding history for a single coin.

        Returns ``(records, parse_errors)``.
        """
        body = {
            "type": "fundingHistory",
            "coin": coin,
            "startTime": start_ms,
            "endTime": end_ms,
        }
        data = await self._post_json(session, self._api_url, body, weight=API_WEIGHT)
        if not isinstance(data, list):
            LOGGER.warning(
                "hl_rest_funding_unexpected_response coin=%s type=%s",
                coin,
                type(data).__name__,
            )
            return [], 0

        records: list[RestFundingRecord] = []
        parse_errors = 0
        for index, entry in enumerate(data):
            try:
                record = parse_rest_funding(entry, coin=coin, allowed_coins=self._coins)
                records.append(record)
            except Exception as error:
                parse_errors += 1
                LOGGER.warning(
                    "hl_rest_funding_parse_error coin=%s idx=%d error=%r",
                    coin,
                    index,
                    error,
                )
        return records, parse_errors
