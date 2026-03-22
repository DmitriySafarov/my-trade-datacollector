from __future__ import annotations

from datetime import datetime, timezone

import asyncpg
import pytest


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source",
    ["bn_rest_ticker_24h", "bn_rest_price", "bn_rest_book_ticker"],
)
async def test_bn_rest_tickers_require_symbol_and_dedup_retries(
    migrated_db: dict[str, object],
    source: str,
) -> None:
    event_time = _utc("2026-03-22T06:00:00+06:00")
    pool = migrated_db["pool"]

    async with pool.acquire() as connection:
        with pytest.raises(asyncpg.NotNullViolationError):
            await connection.execute(
                """
                INSERT INTO bn_tickers (time, source, symbol, last_price)
                VALUES ($1, $2, $3, 10.0)
                """,
                event_time,
                source,
                None,
            )

        for _ in range(2):
            await connection.execute(
                """
                INSERT INTO bn_tickers (time, source, symbol, last_price)
                VALUES ($1, $2, 'ETHUSDT', 10.0)
                ON CONFLICT DO NOTHING
                """,
                event_time,
                source,
            )

        row = await connection.fetchrow(
            """
            SELECT source, symbol, time
            FROM bn_tickers
            WHERE source = $1
            """,
            source,
        )
        row_count = await connection.fetchval(
            "SELECT count(*) FROM bn_tickers WHERE source = $1",
            source,
        )

    assert row_count == 1
    assert row["source"] == source
    assert row["symbol"] == "ETHUSDT"
    assert row["time"] == event_time
