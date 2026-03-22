from __future__ import annotations

from datetime import datetime, timezone

import pytest


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)


@pytest.mark.asyncio
async def test_sentiment_finbert_has_bronze_landing_path(
    migrated_db: dict[str, object],
) -> None:
    event_time = _utc("2026-03-22T06:00:00+06:00")
    replay_time = _utc("2026-03-22T06:05:00+06:00")
    canonical_url = "https://canonical.example.com/articles/etf-headline"
    pool = migrated_db["pool"]

    async with pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO news_sentiment (
                time,
                source,
                article_url,
                canonical_url,
                source_name,
                title,
                sentiment_score,
                sentiment_label
            )
            VALUES (
                $1,
                'sentiment_finbert',
                $2,
                $3,
                'CoinDesk',
                'ETF headline',
                0.91,
                'positive'
            )
            ON CONFLICT DO NOTHING
            """,
            event_time,
            "https://example.com/articles/etf-headline?utm_source=rss",
            canonical_url,
        )
        await connection.execute(
            """
            INSERT INTO news_sentiment (
                time,
                source,
                article_url,
                canonical_url,
                source_name,
                title,
                sentiment_score,
                sentiment_label
            )
            VALUES (
                $1,
                'sentiment_finbert',
                $2,
                $3,
                'CoinDesk',
                'ETF headline',
                0.91,
                'positive'
            )
            ON CONFLICT DO NOTHING
            """,
            replay_time,
            "https://m.example.com/articles/etf-headline/",
            canonical_url,
        )

        rows = await connection.fetch(
            """
            SELECT source, article_url, dedup_url, sentiment_score, time
            FROM news_sentiment
            WHERE dedup_url = $1
            ORDER BY time
            """,
            canonical_url,
        )
        row_count = await connection.fetchval(
            "SELECT count(*) FROM news_sentiment WHERE dedup_url = $1",
            canonical_url,
        )

    assert row_count == 2
    assert all(row["source"] == "sentiment_finbert" for row in rows)
    assert [row["article_url"] for row in rows] == [
        "https://example.com/articles/etf-headline?utm_source=rss",
        "https://m.example.com/articles/etf-headline/",
    ]
    assert all(row["dedup_url"] == canonical_url for row in rows)
    assert all(row["sentiment_score"] == 0.91 for row in rows)
    assert [row["time"] for row in rows] == [event_time, replay_time]


@pytest.mark.asyncio
async def test_sentiment_dedup_url_matches_normalized_registry_identity_without_canonical_url(
    migrated_db: dict[str, object],
) -> None:
    event_time = _utc("2026-03-22T07:00:00+06:00")
    replay_time = _utc("2026-03-22T07:05:00+06:00")
    normalized_url = "https://example.com/articles/etf-headline"
    pool = migrated_db["pool"]

    async with pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO news_sentiment (
                time,
                source,
                article_url,
                source_name,
                title,
                sentiment_score,
                sentiment_label
            )
            VALUES (
                $1,
                'sentiment_finbert',
                $2,
                'CoinDesk',
                'ETF headline',
                0.91,
                'positive'
            )
            ON CONFLICT DO NOTHING
            """,
            event_time,
            "https://example.com/articles/etf-headline/#updates",
        )
        await connection.execute(
            """
            INSERT INTO news_sentiment (
                time,
                source,
                article_url,
                source_name,
                title,
                sentiment_score,
                sentiment_label
            )
            VALUES (
                $1,
                'sentiment_finbert',
                $2,
                'CoinDesk',
                'ETF headline',
                0.91,
                'positive'
            )
            ON CONFLICT DO NOTHING
            """,
            replay_time,
            "https://example.com/articles/etf-headline/",
        )

        row = await connection.fetchrow(
            """
            SELECT source, article_url, dedup_url, sentiment_score, time
            FROM news_sentiment
            WHERE dedup_url = $1
            """,
            normalized_url,
        )
        row_count = await connection.fetchval(
            "SELECT count(*) FROM news_sentiment WHERE dedup_url = $1",
            normalized_url,
        )
        registry_count = await connection.fetchval(
            """
            SELECT count(*)
            FROM news_sentiment_dedup_registry
            WHERE source = 'sentiment_finbert'
              AND dedup_url = $1
            """,
            normalized_url,
        )

    assert row_count == 1
    assert registry_count == 1
    assert row["source"] == "sentiment_finbert"
    assert row["article_url"] == "https://example.com/articles/etf-headline/#updates"
    assert row["dedup_url"] == normalized_url
    assert row["sentiment_score"] == 0.91
    assert row["time"] == event_time
