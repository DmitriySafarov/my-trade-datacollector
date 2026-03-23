from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_silver_views_expose_expected_columns(
    migrated_db: dict[str, object],
) -> None:
    expected = {
        "v_candles": [
            "time",
            "source",
            "exchange",
            "coin",
            "interval",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ],
        "v_funding": ["time", "source", "exchange", "coin", "rate"],
        "v_news_deduped": [
            "time",
            "source",
            "source_name",
            "title",
            "url",
            "canonical_url",
            "dedup_url",
            "summary",
            "related_symbols",
            "sentiment_score",
            "sentiment_label",
            "sentiment_source",
            "duplicate_count",
            "source_count",
        ],
        "v_oi": ["time", "source", "exchange", "coin", "open_interest"],
        "v_orderbook": [
            "time",
            "source",
            "exchange",
            "coin",
            "best_bid",
            "best_ask",
            "spread",
            "imbalance",
        ],
        "v_trades": ["time", "source", "exchange", "coin", "side", "price", "size"],
    }
    pool = migrated_db["pool"]

    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = ANY($1::text[])
            ORDER BY table_name, ordinal_position
            """,
            list(expected),
        )

    columns: dict[str, list[str]] = {}
    for row in rows:
        columns.setdefault(row["table_name"], []).append(row["column_name"])

    assert columns == expected
