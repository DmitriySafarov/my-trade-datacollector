from __future__ import annotations

from datetime import datetime, timezone

import pytest


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)


@pytest.mark.asyncio
async def test_same_source_canonical_rows_keep_distinct_raw_hosts(
    migrated_db: dict[str, object],
) -> None:
    canonical_url = "https://canonical.example.com/articles/etf-headline"
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
            ON CONFLICT DO NOTHING
            """,
            _utc("2026-03-22T08:00:00+06:00"),
            "https://www.example.com/articles/etf-headline?ref=desktop",
            _utc("2026-03-22T08:05:00+06:00"),
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
                ($3, 'sentiment_finbert', $5, $4, 'CoinDesk', 'ETF headline', 0.91, 'positive')
            ON CONFLICT DO NOTHING
            """,
            _utc("2026-03-22T09:00:00+06:00"),
            "https://www.example.com/articles/etf-headline?ref=desktop",
            _utc("2026-03-22T09:05:00+06:00"),
            canonical_url,
            "https://m.example.com/articles/etf-headline?ref=mobile",
        )

        news_rows = await connection.fetch(
            """
            SELECT url, dedup_url
            FROM news
            WHERE source = 'news_rss_coindesk'
            ORDER BY time
            """,
        )
        news_hosts = {
            row["raw_host"]
            for row in await connection.fetch(
                """
                SELECT raw_host
                FROM news_dedup_registry
                WHERE source = 'news_rss_coindesk'
                  AND dedup_url = $1
                """,
                canonical_url,
            )
        }
        sentiment_rows = await connection.fetch(
            """
            SELECT article_url, dedup_url
            FROM news_sentiment
            WHERE source = 'sentiment_finbert'
            ORDER BY time
            """,
        )
        sentiment_hosts = {
            row["raw_host"]
            for row in await connection.fetch(
                """
                SELECT raw_host
                FROM news_sentiment_dedup_registry
                WHERE source = 'sentiment_finbert'
                  AND dedup_url = $1
                """,
                canonical_url,
            )
        }

    assert [row["dedup_url"] for row in news_rows] == [canonical_url, canonical_url]
    assert [row["url"] for row in news_rows] == [
        "https://www.example.com/articles/etf-headline?ref=desktop",
        "https://m.example.com/articles/etf-headline?ref=mobile",
    ]
    assert news_hosts == {"m.example.com", "www.example.com"}
    assert [row["dedup_url"] for row in sentiment_rows] == [
        canonical_url,
        canonical_url,
    ]
    assert [row["article_url"] for row in sentiment_rows] == [
        "https://www.example.com/articles/etf-headline?ref=desktop",
        "https://m.example.com/articles/etf-headline?ref=mobile",
    ]
    assert sentiment_hosts == {"m.example.com", "www.example.com"}
