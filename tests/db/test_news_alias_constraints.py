from __future__ import annotations

from datetime import datetime, timezone

import pytest


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)


@pytest.mark.asyncio
async def test_news_raw_first_then_canonical_later_keeps_hosts_distinct(
    migrated_db: dict[str, object],
) -> None:
    first_time = _utc("2026-03-22T08:00:00+06:00")
    replay_time = _utc("2026-03-22T08:05:00+06:00")
    cross_source_time = _utc("2026-03-22T08:10:00+06:00")
    original_identity = "https://example.com/articles/etf-headline"
    canonical_url = "https://canonical.example.com/articles/etf-headline"
    pool = migrated_db["pool"]

    async with pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO news (time, source, source_name, title, url, ingested_at)
            VALUES ($1, 'news_rss_coindesk', 'CoinDesk', 'ETF headline', $2, $1)
            ON CONFLICT DO NOTHING
            """,
            first_time,
            "https://example.com/articles/etf-headline/",
        )
        await connection.execute(
            """
            INSERT INTO news (
                time,
                source,
                source_name,
                title,
                url,
                canonical_url,
                ingested_at
            )
            VALUES ($1, 'news_rss_coindesk', 'CoinDesk', 'ETF headline', $2, $3, $1)
            ON CONFLICT DO NOTHING
            """,
            replay_time,
            "https://m.example.com/articles/etf-headline?ref=mobile",
            canonical_url,
        )
        await connection.execute(
            """
            INSERT INTO news (
                time,
                source,
                source_name,
                title,
                url,
                canonical_url,
                ingested_at
            )
            VALUES ($1, 'news_finnhub', 'Reuters', 'ETF headline', $2, $3, $1)
            ON CONFLICT DO NOTHING
            """,
            cross_source_time,
            "https://api.example.com/articles/etf-headline",
            canonical_url,
        )

        rows = await connection.fetch(
            """
            SELECT source, url, dedup_url
            FROM news
            WHERE source IN ('news_rss_coindesk', 'news_finnhub')
            ORDER BY source
            """,
        )
        alias_primary = await connection.fetchval(
            """
            SELECT resolved_dedup_url
            FROM news_url_aliases
            WHERE alias_url = $1
            """,
            canonical_url,
        )
        provenance_sources = {
            row["source"]
            for row in await connection.fetch(
                """
                SELECT source
                FROM news_provenance
                WHERE dedup_url = $1
                """,
                original_identity,
            )
        }
        canonical_provenance_sources = {
            row["source"]
            for row in await connection.fetch(
                """
                SELECT source
                FROM news_provenance
                WHERE dedup_url = $1
                """,
                canonical_url,
            )
        }

    assert [row["source"] for row in rows] == [
        "news_finnhub",
        "news_rss_coindesk",
        "news_rss_coindesk",
    ]
    assert rows[0]["dedup_url"] == canonical_url
    assert rows[1]["dedup_url"] == original_identity
    assert rows[2]["dedup_url"] == canonical_url
    assert rows[0]["url"] == "https://api.example.com/articles/etf-headline"
    assert rows[1]["url"] == "https://example.com/articles/etf-headline/"
    assert rows[2]["url"] == "https://m.example.com/articles/etf-headline?ref=mobile"
    assert alias_primary == canonical_url
    assert provenance_sources == {"news_rss_coindesk"}
    assert canonical_provenance_sources == {"news_finnhub", "news_rss_coindesk"}


@pytest.mark.asyncio
async def test_sentiment_raw_first_then_canonical_later_keeps_hosts_distinct(
    migrated_db: dict[str, object],
) -> None:
    first_time = _utc("2026-03-22T09:00:00+06:00")
    replay_time = _utc("2026-03-22T09:05:00+06:00")
    original_identity = "https://example.com/articles/etf-headline"
    canonical_url = "https://canonical.example.com/articles/etf-headline"
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
            first_time,
            "https://example.com/articles/etf-headline/",
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
            "https://m.example.com/articles/etf-headline?ref=mobile",
            canonical_url,
        )

        rows = await connection.fetch(
            """
            SELECT article_url, dedup_url
            FROM news_sentiment
            WHERE source = 'sentiment_finbert'
            ORDER BY time
            """,
        )
        alias_primary = await connection.fetchval(
            """
            SELECT resolved_dedup_url
            FROM news_url_aliases
            WHERE alias_url = $1
            """,
            canonical_url,
        )
        registry_count = await connection.fetchval(
            """
                SELECT count(*)
                FROM news_sentiment_dedup_registry
                WHERE source = 'sentiment_finbert'
                  AND dedup_url = $1
                """,
            original_identity,
        )
        canonical_registry_count = await connection.fetchval(
            """
            SELECT count(*)
            FROM news_sentiment_dedup_registry
            WHERE source = 'sentiment_finbert'
              AND dedup_url = $1
            """,
            canonical_url,
        )

    assert [row["article_url"] for row in rows] == [
        "https://example.com/articles/etf-headline/",
        "https://m.example.com/articles/etf-headline?ref=mobile",
    ]
    assert [row["dedup_url"] for row in rows] == [original_identity, canonical_url]
    assert alias_primary == canonical_url
    assert registry_count == 1
    assert canonical_registry_count == 1
