from __future__ import annotations

from datetime import datetime, timezone

import pytest


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)


@pytest.mark.asyncio
async def test_news_canonical_replay_reuses_raw_first_identity(
    migrated_db: dict[str, object],
) -> None:
    event_time = _utc("2026-03-22T08:00:00+06:00")
    replay_time = _utc("2026-03-22T08:05:00+06:00")
    raw_identity = "https://example.com/articles/etf-headline"
    canonical_identity = "https://canonical.example.com/articles/etf-headline"
    pool = migrated_db["pool"]

    async with pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO news (time, source, source_name, title, url, ingested_at)
            VALUES ($1, 'news_rss_coindesk', 'CoinDesk', 'ETF headline', $2, $1)
            ON CONFLICT DO NOTHING
            """,
            event_time,
            "https://example.com/articles/etf-headline/#updates",
        )
        await connection.execute(
            """
            INSERT INTO news (
                time, source, source_name, title, url, canonical_url, ingested_at
            )
            VALUES ($1, 'news_rss_coindesk', 'CoinDesk', 'ETF headline', $2, $3, $1)
            ON CONFLICT DO NOTHING
            """,
            replay_time,
            "https://example.com/articles/etf-headline/",
            canonical_identity,
        )

        row_count = await connection.fetchval(
            "SELECT count(*) FROM news WHERE source = 'news_rss_coindesk'",
        )
        row = await connection.fetchrow(
            """
            SELECT url, canonical_url, dedup_url, time
            FROM news
            WHERE source = 'news_rss_coindesk'
            """,
        )
        provenance_count = await connection.fetchval(
            """
            SELECT count(*)
            FROM news_provenance
            WHERE source = 'news_rss_coindesk'
              AND dedup_url = $1
            """,
            raw_identity,
        )
        alias_row = await connection.fetchrow(
            """
            SELECT alias_url, resolved_dedup_url
            FROM news_url_aliases
            WHERE alias_url = $1
            """,
            canonical_identity,
        )

    assert row_count == 1
    assert row["url"] == "https://example.com/articles/etf-headline/#updates"
    assert row["canonical_url"] is None
    assert row["dedup_url"] == raw_identity
    assert row["time"] == event_time
    assert provenance_count == 1
    assert dict(alias_row) == {
        "alias_url": canonical_identity,
        "resolved_dedup_url": raw_identity,
    }


@pytest.mark.asyncio
async def test_sentiment_canonical_replay_reuses_raw_first_identity(
    migrated_db: dict[str, object],
) -> None:
    event_time = _utc("2026-03-22T09:00:00+06:00")
    replay_time = _utc("2026-03-22T09:05:00+06:00")
    raw_identity = "https://example.com/articles/etf-headline"
    canonical_identity = "https://canonical.example.com/articles/etf-headline"
    pool = migrated_db["pool"]

    async with pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO news_sentiment (
                time, source, article_url, source_name, title, sentiment_score, sentiment_label
            )
            VALUES ($1, 'sentiment_finbert', $2, 'CoinDesk', 'ETF headline', 0.91, 'positive')
            ON CONFLICT DO NOTHING
            """,
            event_time,
            "https://example.com/articles/etf-headline/#updates",
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
            "https://example.com/articles/etf-headline/",
            canonical_identity,
        )

        row_count = await connection.fetchval(
            "SELECT count(*) FROM news_sentiment WHERE source = 'sentiment_finbert'",
        )
        row = await connection.fetchrow(
            """
            SELECT article_url, canonical_url, dedup_url, time
            FROM news_sentiment
            WHERE source = 'sentiment_finbert'
            """,
        )
        registry_count = await connection.fetchval(
            """
            SELECT count(*)
            FROM news_sentiment_dedup_registry
            WHERE source = 'sentiment_finbert'
              AND dedup_url = $1
            """,
            raw_identity,
        )
        alias_row = await connection.fetchrow(
            """
            SELECT alias_url, resolved_dedup_url
            FROM news_url_aliases
            WHERE alias_url = $1
            """,
            canonical_identity,
        )

    assert row_count == 1
    assert row["article_url"] == "https://example.com/articles/etf-headline/#updates"
    assert row["canonical_url"] is None
    assert row["dedup_url"] == raw_identity
    assert row["time"] == event_time
    assert registry_count == 1
    assert dict(alias_row) == {
        "alias_url": canonical_identity,
        "resolved_dedup_url": raw_identity,
    }
