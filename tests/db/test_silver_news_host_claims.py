from __future__ import annotations

from datetime import datetime, timezone

import pytest


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)


@pytest.mark.asyncio
async def test_v_news_deduped_collapses_same_source_canonical_claimed_hosts(
    migrated_db: dict[str, object],
) -> None:
    canonical_url = "https://canonical.example.com/articles/etf-headline"
    first_time = _utc("2026-03-22T08:00:00+06:00")
    second_time = _utc("2026-03-22T08:05:00+06:00")
    pool = migrated_db["pool"]

    async with pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO news (
                time, source, source_name, title, url, canonical_url, ingested_at
            )
            VALUES
                ($1, 'news_rss_coindesk', 'CoinDesk', 'ETF headline', $2, $4, $1),
                ($3, 'news_rss_coindesk', 'CoinDesk', 'ETF headline', $5, $4, $3)
            """,
            first_time,
            "https://www.example.com/articles/etf-headline?ref=desktop",
            second_time,
            canonical_url,
            "https://m.example.com/articles/etf-headline?ref=mobile",
        )
        await connection.execute(
            """
            INSERT INTO news_sentiment (
                time, source, article_url, canonical_url, source_name, title,
                sentiment_score, sentiment_label
            )
            VALUES
                ($1, 'sentiment_finbert', $2, $4, 'CoinDesk', 'ETF headline', 0.91, 'positive'),
                ($3, 'sentiment_finbert', $5, $4, 'CoinDesk', 'ETF headline', -0.25, 'negative')
            """,
            first_time,
            "https://www.example.com/articles/etf-headline?ref=desktop",
            second_time,
            canonical_url,
            "https://m.example.com/articles/etf-headline?ref=mobile",
        )
        row = await connection.fetchrow(
            """
            SELECT time, source, url, dedup_url, sentiment_score, sentiment_label, sentiment_source, duplicate_count, source_count
            FROM v_news_deduped
            WHERE dedup_url = $1
            """,
            canonical_url,
        )

    assert dict(row) == {
        "time": first_time,
        "source": "news_rss_coindesk",
        "url": "https://www.example.com/articles/etf-headline?ref=desktop",
        "dedup_url": canonical_url,
        "sentiment_score": -0.25,
        "sentiment_label": "negative",
        "sentiment_source": "sentiment_finbert",
        "duplicate_count": 1,
        "source_count": 1,
    }
