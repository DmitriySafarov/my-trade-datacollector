"""Tests for Hyperliquid REST candles snapshot poller."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest

from src.collectors.rest.hl_candles_parsing import SOURCE_ID
from src.collectors.rest.hl_candles_poller import (
    DEFAULT_COINS,
    DEFAULT_LOOKBACK_SECONDS,
    DEFAULT_POLL_SECONDS,
    HyperliquidCandlesPoller,
)


def _make_api_candle(
    coin: str = "ETH",
    interval: str = "1h",
    open_time_ms: int = 1_700_000_000_000,
) -> dict[str, Any]:
    return {
        "t": open_time_ms,
        "T": open_time_ms + 3_600_000,
        "s": coin,
        "i": interval,
        "o": "3450.5",
        "h": "3465.0",
        "l": "3440.0",
        "c": "3455.25",
        "v": "12345.67",
        "n": 5000,
    }


class TestHyperliquidCandlesPollerInit:
    def test_default_configuration(self) -> None:
        poller = HyperliquidCandlesPoller(
            base_url="https://api.hyperliquid.xyz",
            pool=AsyncMock(spec=asyncpg.Pool),
        )
        assert poller.name == "hl_rest_candles"
        assert poller.source_ids == (SOURCE_ID,)
        assert poller._interval_seconds == DEFAULT_POLL_SECONDS
        assert poller._coins == DEFAULT_COINS
        assert poller._lookback_seconds == DEFAULT_LOOKBACK_SECONDS

    def test_custom_coins_and_intervals(self) -> None:
        poller = HyperliquidCandlesPoller(
            base_url="https://api.hyperliquid.xyz",
            pool=AsyncMock(spec=asyncpg.Pool),
            coins=("eth",),
            intervals=("1h", "4h"),
        )
        assert poller._coins == ("ETH",)
        assert poller._intervals == ("1h", "4h")

    def test_api_url_trailing_slash_stripped(self) -> None:
        poller = HyperliquidCandlesPoller(
            base_url="https://api.hyperliquid.xyz/",
            pool=AsyncMock(spec=asyncpg.Pool),
        )
        assert poller._api_url == "https://api.hyperliquid.xyz/info"

    def test_health_snapshot(self) -> None:
        poller = HyperliquidCandlesPoller(
            base_url="https://api.hyperliquid.xyz",
            pool=AsyncMock(spec=asyncpg.Pool),
        )
        snap = poller.health_snapshot()
        assert snap["name"] == "hl_rest_candles"
        assert snap["source_ids"] == [SOURCE_ID]
        assert snap["total_polls"] == 0


class TestHyperliquidCandlesPollerDbIntegration:
    """DB-backed integration tests proving the full poll → parse → write path."""

    @pytest.mark.asyncio
    async def test_poll_writes_candles_to_db(
        self,
        migrated_db: dict[str, object],
    ) -> None:
        """Full integration: mock HTTP, real DB write, verify rows."""
        pool: asyncpg.Pool = migrated_db["pool"]  # type: ignore[assignment]

        candles = [
            _make_api_candle("ETH", "1h", 1_700_000_000_000),
            _make_api_candle("ETH", "1h", 1_700_003_600_000),
        ]

        mock_response = AsyncMock()
        mock_response.json = AsyncMock(return_value=candles)
        mock_response.raise_for_status = lambda: None
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = lambda *a, **kw: mock_response

        poller = HyperliquidCandlesPoller(
            base_url="https://api.hyperliquid.xyz",
            pool=pool,
            coins=("ETH",),
            intervals=("1h",),
            interval_seconds=0.05,
            lookback_seconds=86400,
            jitter_ratio=0.0,
        )
        await poller._poll(mock_session)

        async with pool.acquire() as conn:
            count = await conn.fetchval(
                "SELECT count(*) FROM hl_rest_candles WHERE source = $1",
                SOURCE_ID,
            )
        assert count == 2

    @pytest.mark.asyncio
    async def test_duplicate_candles_deduplicated(
        self,
        migrated_db: dict[str, object],
    ) -> None:
        """Same candles written twice — ON CONFLICT DO NOTHING prevents dupes."""
        pool: asyncpg.Pool = migrated_db["pool"]  # type: ignore[assignment]

        candles = [_make_api_candle("ETH", "1h", 1_700_000_000_000)]

        mock_response = AsyncMock()
        mock_response.json = AsyncMock(return_value=candles)
        mock_response.raise_for_status = lambda: None
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = lambda *a, **kw: mock_response

        poller = HyperliquidCandlesPoller(
            base_url="https://api.hyperliquid.xyz",
            pool=pool,
            coins=("ETH",),
            intervals=("1h",),
            interval_seconds=0.05,
            lookback_seconds=86400,
            jitter_ratio=0.0,
        )
        # Write the same candle twice.
        await poller._poll(mock_session)
        await poller._poll(mock_session)

        async with pool.acquire() as conn:
            count = await conn.fetchval(
                "SELECT count(*) FROM hl_rest_candles WHERE source = $1",
                SOURCE_ID,
            )
        assert count == 1  # Deduped

    @pytest.mark.asyncio
    async def test_column_values_match_parsed_record(
        self,
        migrated_db: dict[str, object],
    ) -> None:
        """Verify all written column values match the parsed candle."""
        pool: asyncpg.Pool = migrated_db["pool"]  # type: ignore[assignment]

        candles = [_make_api_candle("BTC", "4h", 1_700_000_000_000)]

        mock_response = AsyncMock()
        mock_response.json = AsyncMock(return_value=candles)
        mock_response.raise_for_status = lambda: None
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = lambda *a, **kw: mock_response

        poller = HyperliquidCandlesPoller(
            base_url="https://api.hyperliquid.xyz",
            pool=pool,
            coins=("BTC",),
            intervals=("4h",),
            interval_seconds=0.05,
            lookback_seconds=86400,
            jitter_ratio=0.0,
        )
        await poller._poll(mock_session)

        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM hl_rest_candles WHERE source = $1 LIMIT 1",
                SOURCE_ID,
            )

        assert row is not None
        assert row["source"] == SOURCE_ID
        assert row["coin"] == "BTC"
        assert row["interval"] == "4h"
        assert row["time"] == row["open_time"]  # CHECK constraint
        assert row["open"] == 3450.5
        assert row["high"] == 3465.0
        assert row["low"] == 3440.0
        assert row["close"] == 3455.25
        assert row["volume"] == 12345.67
        assert row["trades_count"] == 5000

    @pytest.mark.asyncio
    async def test_poller_lifecycle_start_stop(
        self,
        migrated_db: dict[str, object],
    ) -> None:
        """Start, run one poll cycle, stop — verifies BaseRestPoller lifecycle."""
        pool: asyncpg.Pool = migrated_db["pool"]  # type: ignore[assignment]

        candles = [_make_api_candle("ETH", "1h")]

        mock_response = AsyncMock()
        mock_response.json = AsyncMock(return_value=candles)
        mock_response.raise_for_status = lambda: None
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        poller = HyperliquidCandlesPoller(
            base_url="https://api.hyperliquid.xyz",
            pool=pool,
            coins=("ETH",),
            intervals=("1h",),
            interval_seconds=0.05,
            lookback_seconds=86400,
            jitter_ratio=0.0,
        )

        with patch.object(
            type(poller),
            "start",
            wraps=poller.start,
        ):
            # Patch aiohttp.ClientSession to return our mock
            mock_session_ctx = AsyncMock()
            mock_session_ctx.__aenter__ = AsyncMock(
                return_value=AsyncMock(
                    post=lambda *a, **kw: mock_response,
                    timeout=None,
                )
            )
            mock_session_ctx.__aexit__ = AsyncMock(return_value=False)

            with patch("aiohttp.ClientSession", return_value=mock_session_ctx):
                task = asyncio.create_task(poller.start())
                await asyncio.wait_for(poller.wait_ready(), timeout=10.0)
                snap = poller.health_snapshot()
                assert snap["total_polls"] >= 1
                assert snap["total_errors"] == 0
                await poller.stop()
                await asyncio.wait_for(task, timeout=5.0)

    @pytest.mark.asyncio
    async def test_parse_error_skips_bad_candle_writes_good(
        self,
        migrated_db: dict[str, object],
    ) -> None:
        """A malformed candle is skipped; valid ones still written."""
        pool: asyncpg.Pool = migrated_db["pool"]  # type: ignore[assignment]

        candles = [
            {"bad": "data"},  # Will fail parsing
            _make_api_candle("ETH", "1h", 1_700_000_000_000),
        ]

        mock_response = AsyncMock()
        mock_response.json = AsyncMock(return_value=candles)
        mock_response.raise_for_status = lambda: None
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = lambda *a, **kw: mock_response

        poller = HyperliquidCandlesPoller(
            base_url="https://api.hyperliquid.xyz",
            pool=pool,
            coins=("ETH",),
            intervals=("1h",),
            interval_seconds=0.05,
            lookback_seconds=86400,
            jitter_ratio=0.0,
        )
        await poller._poll(mock_session)

        async with pool.acquire() as conn:
            count = await conn.fetchval(
                "SELECT count(*) FROM hl_rest_candles WHERE source = $1",
                SOURCE_ID,
            )
        assert count == 1  # Only the valid candle

    @pytest.mark.asyncio
    async def test_multiple_coins_and_intervals(
        self,
        migrated_db: dict[str, object],
    ) -> None:
        """Fetches candles for each coin × interval combination."""
        pool: asyncpg.Pool = migrated_db["pool"]  # type: ignore[assignment]

        call_log: list[dict] = []

        def make_response(candles: list) -> AsyncMock:
            resp = AsyncMock()
            resp.json = AsyncMock(return_value=candles)
            resp.raise_for_status = lambda: None
            resp.__aenter__ = AsyncMock(return_value=resp)
            resp.__aexit__ = AsyncMock(return_value=False)
            return resp

        def mock_post(url: str, json: dict | None = None, **kw: Any) -> Any:
            call_log.append(json or {})
            coin = json["req"]["coin"] if json else "ETH"
            interval = json["req"]["interval"] if json else "1h"
            # Each request gets a unique candle
            ts = 1_700_000_000_000 + len(call_log) * 3_600_000
            return make_response([_make_api_candle(coin, interval, ts)])

        mock_session = AsyncMock()
        mock_session.post = mock_post

        poller = HyperliquidCandlesPoller(
            base_url="https://api.hyperliquid.xyz",
            pool=pool,
            coins=("ETH", "BTC"),
            intervals=("1h", "4h"),
            interval_seconds=0.05,
            lookback_seconds=86400,
            jitter_ratio=0.0,
        )
        await poller._poll(mock_session)

        # 2 coins × 2 intervals = 4 API calls
        assert len(call_log) == 4

        async with pool.acquire() as conn:
            count = await conn.fetchval(
                "SELECT count(*) FROM hl_rest_candles WHERE source = $1",
                SOURCE_ID,
            )
        assert count == 4


class TestHyperliquidCandlesPollerErrorHandling:
    """Tests for partial and total failure scenarios."""

    @pytest.mark.asyncio
    async def test_majority_failures_raises(
        self,
        migrated_db: dict[str, object],
    ) -> None:
        """When >50% fetches fail, _poll raises to trigger base class backoff."""
        pool: asyncpg.Pool = migrated_db["pool"]  # type: ignore[assignment]

        call_count = 0

        def mock_post(url: str, json: dict | None = None, **kw: Any) -> Any:
            nonlocal call_count
            call_count += 1
            # Fail 3 out of 4 requests (75% error rate)
            if call_count <= 3:
                raise ConnectionError("simulated API error")
            resp = AsyncMock()
            resp.json = AsyncMock(
                return_value=[_make_api_candle("BTC", "4h", 1_700_000_000_000)]
            )
            resp.raise_for_status = lambda: None
            resp.__aenter__ = AsyncMock(return_value=resp)
            resp.__aexit__ = AsyncMock(return_value=False)
            return resp

        mock_session = AsyncMock()
        mock_session.post = mock_post

        poller = HyperliquidCandlesPoller(
            base_url="https://api.hyperliquid.xyz",
            pool=pool,
            coins=("ETH", "BTC"),
            intervals=("1h", "4h"),
            interval_seconds=0.05,
            lookback_seconds=86400,
            jitter_ratio=0.0,
        )
        with pytest.raises(RuntimeError, match="fetches failed"):
            await poller._poll(mock_session)

    @pytest.mark.asyncio
    async def test_minority_failures_succeeds_with_warning(
        self,
        migrated_db: dict[str, object],
    ) -> None:
        """When <=50% fetches fail, _poll succeeds (no raise)."""
        pool: asyncpg.Pool = migrated_db["pool"]  # type: ignore[assignment]

        call_count = 0

        def mock_post(url: str, json: dict | None = None, **kw: Any) -> Any:
            nonlocal call_count
            call_count += 1
            # Fail 1 out of 4 requests (25% error rate)
            if call_count == 1:
                raise ConnectionError("simulated API error")
            coin = json["req"]["coin"] if json else "ETH"
            interval = json["req"]["interval"] if json else "1h"
            ts = 1_700_000_000_000 + call_count * 3_600_000
            resp = AsyncMock()
            resp.json = AsyncMock(return_value=[_make_api_candle(coin, interval, ts)])
            resp.raise_for_status = lambda: None
            resp.__aenter__ = AsyncMock(return_value=resp)
            resp.__aexit__ = AsyncMock(return_value=False)
            return resp

        mock_session = AsyncMock()
        mock_session.post = mock_post

        poller = HyperliquidCandlesPoller(
            base_url="https://api.hyperliquid.xyz",
            pool=pool,
            coins=("ETH", "BTC"),
            intervals=("1h", "4h"),
            interval_seconds=0.05,
            lookback_seconds=86400,
            jitter_ratio=0.0,
        )
        # Should NOT raise — minority failures are tolerated.
        await poller._poll(mock_session)

        async with pool.acquire() as conn:
            count = await conn.fetchval(
                "SELECT count(*) FROM hl_rest_candles WHERE source = $1",
                SOURCE_ID,
            )
        assert count == 3  # 4 total - 1 failed = 3 written
