from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from ._migration_upgrade import (
    apply_repair_migrations,
    compress_tables,
    upgrade_db,
)


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)


@pytest.mark.asyncio
async def test_v_news_deduped_uses_repaired_identity_for_legacy_rows(
    tmp_path: Path,
) -> None:
    canonical_url = "https://canonical.example.com/articles/etf-headline"
    repaired_identity = "https://example.com/articles/etf-headline"

    async with upgrade_db(tmp_path) as pool:
        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO news (
                    time, source, source_name, title, url, canonical_url, ingested_at
                )
                VALUES
                    ($1, 'news_rss_coindesk', 'CoinDesk', 'ETF headline', $2, NULL, $3),
                    ($4, 'news_rss_coindesk', 'CoinDesk', 'ETF headline', $5, $6, $7)
                """,
                _utc("2026-03-22T06:00:00+00:00"),
                "https://example.com/articles/etf-headline/#updates",
                _utc("2026-03-22T06:10:00+00:00"),
                _utc("2026-03-22T06:01:00+00:00"),
                "https://example.com/articles/etf-headline/",
                canonical_url,
                _utc("2026-03-22T06:11:00+00:00"),
            )
            await connection.execute(
                """
                INSERT INTO news_sentiment (
                    time, source, article_url, canonical_url, source_name, title,
                    sentiment_score, sentiment_label, ingested_at
                )
                VALUES
                    ($1, 'sentiment_finbert', $2, NULL, 'CoinDesk', 'ETF headline', 0.40, 'neutral', $3),
                    ($4, 'sentiment_finbert', $5, $6, 'CoinDesk', 'ETF headline', 0.91, 'positive', $7)
                """,
                _utc("2026-03-22T06:02:00+00:00"),
                "https://example.com/articles/etf-headline/#updates",
                _utc("2026-03-22T06:12:00+00:00"),
                _utc("2026-03-22T06:03:00+00:00"),
                "https://example.com/articles/etf-headline/",
                canonical_url,
                _utc("2026-03-22T06:13:00+00:00"),
            )

        await compress_tables(pool, "news", "news_sentiment")
        await apply_repair_migrations(pool, tmp_path, "silver_news_identity_repairs")

        async with pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT time, source, dedup_url, sentiment_label, sentiment_source, duplicate_count, source_count
                FROM v_news_deduped
                WHERE dedup_url = $1
                """,
                repaired_identity,
            )

    assert dict(row) == {
        "time": _utc("2026-03-22T06:00:00+00:00"),
        "source": "news_rss_coindesk",
        "dedup_url": repaired_identity,
        "sentiment_label": "neutral",
        "sentiment_source": "sentiment_finbert",
        "duplicate_count": 0,
        "source_count": 1,
    }


@pytest.mark.asyncio
async def test_v_news_deduped_falls_back_to_latest_sentiment_and_counts_publishers(
    migrated_db: dict[str, object],
) -> None:
    canonical_url = "https://canonical.example.com/articles/publisher-headline"
    news_time = _utc("2026-03-22T07:00:00+00:00")
    second_news_time = _utc("2026-03-22T07:01:00+00:00")
    pool = migrated_db["pool"]

    async with pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO news (
                time, source, source_name, title, url, canonical_url, ingested_at
            )
            VALUES
                ($1, 'news_rss_coindesk', 'Reuters', 'Publisher headline', $2, $5, $1),
                ($3, 'news_finnhub_general', 'Reuters', 'Publisher headline', $4, $5, $3)
            """,
            news_time,
            "https://www.example.com/articles/publisher-headline?ref=rss",
            second_news_time,
            "https://api.example.com/articles/publisher-headline?ref=finnhub",
            canonical_url,
        )
        await connection.execute(
            """
            INSERT INTO news_sentiment (
                time, source, article_url, canonical_url, source_name, title,
                sentiment_score, sentiment_label
            )
            VALUES
                ($1, 'sentiment_finbert', $2, $5, 'Reuters', 'Publisher headline', 0.10, 'neutral'),
                ($3, 'sentiment_finbert', $4, $5, 'Reuters', 'Publisher headline', -0.35, 'negative')
            """,
            _utc("2026-03-22T07:02:00+00:00"),
            "https://www.example.com/articles/publisher-headline?desk",
            _utc("2026-03-22T07:03:00+00:00"),
            "https://m.example.com/articles/publisher-headline?mobile",
            canonical_url,
        )
        row = await connection.fetchrow(
            """
            SELECT
                time,
                source,
                source_name,
                url,
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
        "source_name": "Reuters",
        "url": "https://www.example.com/articles/publisher-headline?ref=rss",
        "dedup_url": canonical_url,
        "sentiment_score": -0.35,
        "sentiment_label": "negative",
        "sentiment_source": "sentiment_finbert",
        "duplicate_count": 1,
        "source_count": 1,
    }
