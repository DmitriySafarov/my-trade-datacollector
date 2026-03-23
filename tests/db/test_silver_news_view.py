from __future__ import annotations

from datetime import datetime, timezone

import pytest


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)


@pytest.mark.asyncio
async def test_v_news_deduped_uses_earliest_primary_row_and_latest_sentiment(
    migrated_db: dict[str, object],
) -> None:
    news_time = _utc("2026-03-22T10:20:00+06:00")
    replay_time = _utc("2026-03-22T10:21:00+06:00")
    sentiment_time = _utc("2026-03-22T10:22:00+06:00")
    canonical_url = "https://canonical.example.com/articles/etf-headline"
    pool = migrated_db["pool"]

    async with pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO news (
                time, source, source_name, title, url, canonical_url, ingested_at
            )
            VALUES
                ($1, 'news_rss_coindesk', 'CoinDesk', 'ETF headline', $2, $3, $1),
                ($4, 'news_rss_coindesk', 'CoinDesk', 'ETF headline', $5, $3, $4),
                ($4, 'news_finnhub', 'Reuters', 'ETF headline', $6, $3, $4)
            """,
            news_time,
            "https://example.com/articles/etf-headline?utm_source=rss",
            canonical_url,
            replay_time,
            "https://example.com/articles/etf-headline?ref=mobile",
            "https://api.example.com/articles/etf-headline?ref=finnhub",
        )
        await connection.execute(
            """
            INSERT INTO news_sentiment (
                time, source, article_url, canonical_url, source_name, title,
                sentiment_score, sentiment_label
            )
            VALUES (
                $1,
                'sentiment_finbert',
                'https://example.com/articles/etf-headline?model=finbert',
                $2,
                'CoinDesk',
                'ETF headline',
                0.91,
                'positive'
            )
            """,
            sentiment_time,
            canonical_url,
        )
        row = await connection.fetchrow(
            """
            SELECT
                time,
                source,
                source_name,
                url,
                canonical_url,
                dedup_url,
                sentiment_score,
                sentiment_label,
                sentiment_source,
                duplicate_count,
                source_count
            FROM v_news_deduped
            WHERE dedup_url = $1
            """,
            canonical_url,
        )

    assert dict(row) == {
        "time": news_time,
        "source": "news_rss_coindesk",
        "source_name": "CoinDesk",
        "url": "https://example.com/articles/etf-headline?utm_source=rss",
        "canonical_url": canonical_url,
        "dedup_url": canonical_url,
        "sentiment_score": 0.91,
        "sentiment_label": "positive",
        "sentiment_source": "sentiment_finbert",
        "duplicate_count": 1,
        "source_count": 2,
    }


@pytest.mark.asyncio
async def test_v_news_deduped_respects_host_aware_dedup_identity(
    migrated_db: dict[str, object],
) -> None:
    event_time = _utc("2026-03-22T11:00:00+06:00")
    pool = migrated_db["pool"]

    async with pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO news (time, source, source_name, title, url, ingested_at)
            VALUES
                ($1, 'news_finnhub_general', 'Reuters', 'Macro headline', $2, $1),
                ($1, 'news_finnhub_general', 'Bloomberg', 'Macro headline', $3, $1)
            """,
            event_time,
            "https://www.reuters.com/world/same-path",
            "https://www.bloomberg.com/world/same-path",
        )
        rows = await connection.fetch(
            """
            SELECT dedup_url, source_count, duplicate_count
            FROM v_news_deduped
            WHERE time = $1
            ORDER BY dedup_url
            """,
            event_time,
        )

    assert [dict(row) for row in rows] == [
        {
            "dedup_url": "https://www.bloomberg.com/world/same-path",
            "source_count": 1,
            "duplicate_count": 0,
        },
        {
            "dedup_url": "https://www.reuters.com/world/same-path",
            "source_count": 1,
            "duplicate_count": 0,
        },
    ]
