"""Tests for Hyperliquid REST funding history poller."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest

from src.collectors.rest.hl_funding_parsing import SOURCE_ID
from src.collectors.rest.hl_funding_poller import (
    DEFAULT_COINS,
    DEFAULT_LOOKBACK_SECONDS,
    DEFAULT_POLL_SECONDS,
    HyperliquidFundingPoller,
)


def _make_api_funding(
    time_ms: int = 1_710_780_000_000,
    funding: str = "0.0001",
    premium: str = "0.00005",
) -> dict[str, Any]:
    return {
        "time": time_ms,
        "funding": funding,
        "premium": premium,
    }


class TestHyperliquidFundingPollerInit:
    def test_default_configuration(self) -> None:
        poller = HyperliquidFundingPoller(
            base_url="https://api.hyperliquid.xyz",
            pool=AsyncMock(spec=asyncpg.Pool),
        )
        assert poller.name == "hl_rest_funding"
        assert poller.source_ids == (SOURCE_ID,)
        assert poller._interval_seconds == DEFAULT_POLL_SECONDS
        assert poller._coins == DEFAULT_COINS
        assert poller._lookback_seconds == DEFAULT_LOOKBACK_SECONDS

    def test_custom_coins(self) -> None:
        poller = HyperliquidFundingPoller(
            base_url="https://api.hyperliquid.xyz",
            pool=AsyncMock(spec=asyncpg.Pool),
            coins=("eth",),
        )
        assert poller._coins == ("ETH",)

    def test_api_url_trailing_slash_stripped(self) -> None:
        poller = HyperliquidFundingPoller(
            base_url="https://api.hyperliquid.xyz/",
            pool=AsyncMock(spec=asyncpg.Pool),
        )
        assert poller._api_url == "https://api.hyperliquid.xyz/info"

    def test_health_snapshot(self) -> None:
        poller = HyperliquidFundingPoller(
            base_url="https://api.hyperliquid.xyz",
            pool=AsyncMock(spec=asyncpg.Pool),
        )
        snap = poller.health_snapshot()
        assert snap["name"] == "hl_rest_funding"
        assert snap["source_ids"] == [SOURCE_ID]
        assert snap["total_polls"] == 0


class TestHyperliquidFundingPollerDbIntegration:
    """DB-backed integration tests proving the full poll → parse → write path."""

    @pytest.mark.asyncio
    async def test_poll_writes_funding_to_db(
        self,
        migrated_db: dict[str, object],
    ) -> None:
        """Full integration: mock HTTP, real DB write, verify rows."""
        pool: asyncpg.Pool = migrated_db["pool"]  # type: ignore[assignment]

        records = [
            _make_api_funding(1_710_780_000_000, "0.0001", "0.00005"),
            _make_api_funding(1_710_783_600_000, "0.00012", "0.00006"),
        ]

        mock_response = AsyncMock()
        mock_response.json = AsyncMock(return_value=records)
        mock_response.raise_for_status = lambda: None
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = lambda *a, **kw: mock_response

        poller = HyperliquidFundingPoller(
            base_url="https://api.hyperliquid.xyz",
            pool=pool,
            coins=("ETH",),
            interval_seconds=0.05,
            lookback_seconds=172800,
            jitter_ratio=0.0,
        )
        await poller._poll(mock_session)

        async with pool.acquire() as conn:
            count = await conn.fetchval(
                "SELECT count(*) FROM hl_rest_funding WHERE source = $1",
                SOURCE_ID,
            )
        assert count == 2

    @pytest.mark.asyncio
    async def test_duplicate_funding_deduplicated(
        self,
        migrated_db: dict[str, object],
    ) -> None:
        """Same records written twice — ON CONFLICT DO NOTHING prevents dupes."""
        pool: asyncpg.Pool = migrated_db["pool"]  # type: ignore[assignment]

        records = [_make_api_funding(1_710_780_000_000)]

        mock_response = AsyncMock()
        mock_response.json = AsyncMock(return_value=records)
        mock_response.raise_for_status = lambda: None
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = lambda *a, **kw: mock_response

        poller = HyperliquidFundingPoller(
            base_url="https://api.hyperliquid.xyz",
            pool=pool,
            coins=("ETH",),
            interval_seconds=0.05,
            lookback_seconds=172800,
            jitter_ratio=0.0,
        )
        await poller._poll(mock_session)
        await poller._poll(mock_session)

        async with pool.acquire() as conn:
            count = await conn.fetchval(
                "SELECT count(*) FROM hl_rest_funding WHERE source = $1",
                SOURCE_ID,
            )
        assert count == 1

    @pytest.mark.asyncio
    async def test_column_values_match_parsed_record(
        self,
        migrated_db: dict[str, object],
    ) -> None:
        """Verify all written column values match the parsed funding record."""
        pool: asyncpg.Pool = migrated_db["pool"]  # type: ignore[assignment]

        records = [_make_api_funding(1_710_780_000_000, "0.0003", "-0.0001")]

        mock_response = AsyncMock()
        mock_response.json = AsyncMock(return_value=records)
        mock_response.raise_for_status = lambda: None
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = lambda *a, **kw: mock_response

        poller = HyperliquidFundingPoller(
            base_url="https://api.hyperliquid.xyz",
            pool=pool,
            coins=("BTC",),
            interval_seconds=0.05,
            lookback_seconds=172800,
            jitter_ratio=0.0,
        )
        await poller._poll(mock_session)

        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM hl_rest_funding WHERE source = $1 LIMIT 1",
                SOURCE_ID,
            )

        assert row is not None
        assert row["source"] == SOURCE_ID
        assert row["coin"] == "BTC"
        assert row["funding"] == 0.0003
        assert row["premium"] == -0.0001
        assert row["mark_price"] is None
        assert row["oracle_price"] is None

    @pytest.mark.asyncio
    async def test_poller_lifecycle_start_stop(
        self,
        migrated_db: dict[str, object],
    ) -> None:
        """Start, run one poll cycle, stop — verifies BaseRestPoller lifecycle."""
        pool: asyncpg.Pool = migrated_db["pool"]  # type: ignore[assignment]

        records = [_make_api_funding()]

        mock_response = AsyncMock()
        mock_response.json = AsyncMock(return_value=records)
        mock_response.raise_for_status = lambda: None
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        poller = HyperliquidFundingPoller(
            base_url="https://api.hyperliquid.xyz",
            pool=pool,
            coins=("ETH",),
            interval_seconds=0.05,
            lookback_seconds=172800,
            jitter_ratio=0.0,
        )

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
    async def test_parse_error_skips_bad_record_writes_good(
        self,
        migrated_db: dict[str, object],
    ) -> None:
        """A malformed record is skipped; valid ones still written."""
        pool: asyncpg.Pool = migrated_db["pool"]  # type: ignore[assignment]

        records = [
            {"bad": "data"},  # Will fail parsing
            _make_api_funding(1_710_780_000_000),
        ]

        mock_response = AsyncMock()
        mock_response.json = AsyncMock(return_value=records)
        mock_response.raise_for_status = lambda: None
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = lambda *a, **kw: mock_response

        poller = HyperliquidFundingPoller(
            base_url="https://api.hyperliquid.xyz",
            pool=pool,
            coins=("ETH",),
            interval_seconds=0.05,
            lookback_seconds=172800,
            jitter_ratio=0.0,
        )
        await poller._poll(mock_session)

        async with pool.acquire() as conn:
            count = await conn.fetchval(
                "SELECT count(*) FROM hl_rest_funding WHERE source = $1",
                SOURCE_ID,
            )
        assert count == 1

    @pytest.mark.asyncio
    async def test_multiple_coins(
        self,
        migrated_db: dict[str, object],
    ) -> None:
        """Fetches funding for each coin separately."""
        pool: asyncpg.Pool = migrated_db["pool"]  # type: ignore[assignment]

        call_log: list[dict] = []

        def make_response(records: list) -> AsyncMock:
            resp = AsyncMock()
            resp.json = AsyncMock(return_value=records)
            resp.raise_for_status = lambda: None
            resp.__aenter__ = AsyncMock(return_value=resp)
            resp.__aexit__ = AsyncMock(return_value=False)
            return resp

        def mock_post(url: str, json: dict | None = None, **kw: Any) -> Any:
            call_log.append(json or {})
            ts = 1_710_780_000_000 + len(call_log) * 3_600_000
            return make_response([_make_api_funding(ts)])

        mock_session = AsyncMock()
        mock_session.post = mock_post

        poller = HyperliquidFundingPoller(
            base_url="https://api.hyperliquid.xyz",
            pool=pool,
            coins=("ETH", "BTC"),
            interval_seconds=0.05,
            lookback_seconds=172800,
            jitter_ratio=0.0,
        )
        await poller._poll(mock_session)

        # 2 coins = 2 API calls
        assert len(call_log) == 2

        async with pool.acquire() as conn:
            count = await conn.fetchval(
                "SELECT count(*) FROM hl_rest_funding WHERE source = $1",
                SOURCE_ID,
            )
        assert count == 2


class TestHyperliquidFundingPollerErrorHandling:
    """Tests for failure scenarios."""

    @pytest.mark.asyncio
    async def test_all_failures_raises(
        self,
        migrated_db: dict[str, object],
    ) -> None:
        """When all fetches fail, _poll raises."""
        pool: asyncpg.Pool = migrated_db["pool"]  # type: ignore[assignment]

        def mock_post(url: str, json: dict | None = None, **kw: Any) -> Any:
            raise ConnectionError("simulated API error")

        mock_session = AsyncMock()
        mock_session.post = mock_post

        poller = HyperliquidFundingPoller(
            base_url="https://api.hyperliquid.xyz",
            pool=pool,
            coins=("ETH", "BTC"),
            interval_seconds=0.05,
            lookback_seconds=172800,
            jitter_ratio=0.0,
        )
        with pytest.raises(RuntimeError, match="fetches failed"):
            await poller._poll(mock_session)

    @pytest.mark.asyncio
    async def test_partial_failure_succeeds(
        self,
        migrated_db: dict[str, object],
    ) -> None:
        """When minority of coins fail (<50%), _poll does not raise."""
        pool: asyncpg.Pool = migrated_db["pool"]  # type: ignore[assignment]

        call_count = 0

        def mock_post(url: str, json: dict | None = None, **kw: Any) -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("simulated API error")
            ts = 1_710_780_000_000 + call_count * 3_600_000
            resp = AsyncMock()
            resp.json = AsyncMock(return_value=[_make_api_funding(ts)])
            resp.raise_for_status = lambda: None
            resp.__aenter__ = AsyncMock(return_value=resp)
            resp.__aexit__ = AsyncMock(return_value=False)
            return resp

        mock_session = AsyncMock()
        mock_session.post = mock_post

        poller = HyperliquidFundingPoller(
            base_url="https://api.hyperliquid.xyz",
            pool=pool,
            coins=("ETH", "BTC", "SOL"),
            interval_seconds=0.05,
            lookback_seconds=172800,
            jitter_ratio=0.0,
        )
        # Should NOT raise — 1/3 failure < 50% threshold
        await poller._poll(mock_session)

        async with pool.acquire() as conn:
            count = await conn.fetchval(
                "SELECT count(*) FROM hl_rest_funding WHERE source = $1",
                SOURCE_ID,
            )
        assert count == 2

    @pytest.mark.asyncio
    async def test_half_failure_raises_degraded(
        self,
        migrated_db: dict[str, object],
    ) -> None:
        """When exactly 50% of coins fail, _poll raises as degraded."""
        pool: asyncpg.Pool = migrated_db["pool"]  # type: ignore[assignment]

        call_count = 0

        def mock_post(url: str, json: dict | None = None, **kw: Any) -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("simulated API error")
            resp = AsyncMock()
            resp.json = AsyncMock(return_value=[_make_api_funding(1_710_780_000_000)])
            resp.raise_for_status = lambda: None
            resp.__aenter__ = AsyncMock(return_value=resp)
            resp.__aexit__ = AsyncMock(return_value=False)
            return resp

        mock_session = AsyncMock()
        mock_session.post = mock_post

        poller = HyperliquidFundingPoller(
            base_url="https://api.hyperliquid.xyz",
            pool=pool,
            coins=("ETH", "BTC"),
            interval_seconds=0.05,
            lookback_seconds=172800,
            jitter_ratio=0.0,
        )
        # Should raise — 1/2 failure = 50% hits degraded threshold
        with pytest.raises(RuntimeError, match="fetches failed"):
            await poller._poll(mock_session)

    @pytest.mark.asyncio
    async def test_unexpected_response_type_handled(
        self,
        migrated_db: dict[str, object],
    ) -> None:
        """Non-list API response is handled gracefully."""
        pool: asyncpg.Pool = migrated_db["pool"]  # type: ignore[assignment]

        mock_response = AsyncMock()
        mock_response.json = AsyncMock(return_value={"error": "bad request"})
        mock_response.raise_for_status = lambda: None
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = lambda *a, **kw: mock_response

        poller = HyperliquidFundingPoller(
            base_url="https://api.hyperliquid.xyz",
            pool=pool,
            coins=("ETH",),
            interval_seconds=0.05,
            lookback_seconds=172800,
            jitter_ratio=0.0,
        )
        # Should not raise — just logs warning and writes 0 records
        await poller._poll(mock_session)

        async with pool.acquire() as conn:
            count = await conn.fetchval(
                "SELECT count(*) FROM hl_rest_funding WHERE source = $1",
                SOURCE_ID,
            )
        assert count == 0
