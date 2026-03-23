from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from ._migration_upgrade import apply_repair_migrations, upgrade_db


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)


@pytest.mark.asyncio
async def test_v_news_deduped_uses_repaired_identity_and_publisher_counts_on_upgrade(
    tmp_path: Path,
) -> None:
    raw_identity = "https://www.example.com/articles/etf-headline"
    canonical_url = "https://canonical.example.com/articles/etf-headline"

    async with upgrade_db(tmp_path) as pool:
        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO news (
                    time, source, source_name, title, url, canonical_url, ingested_at
                )
                VALUES
                    ($1, 'news_rss_coindesk', 'CoinDesk', 'ETF headline', $2, NULL, $1),
                    ($3, 'news_rss_coindesk', 'CoinDesk', 'ETF headline', $4, $5, $3),
                    ($6, 'news_finnhub', 'CoinDesk', 'ETF headline', $7, $2, $6)
                """,
                _utc("2026-03-22T09:00:00+06:00"),
                raw_identity,
                _utc("2026-03-22T09:05:00+06:00"),
                raw_identity,
                canonical_url,
                _utc("2026-03-22T09:10:00+06:00"),
                "https://api.example.com/articles/etf-headline?ref=finnhub",
            )
            await connection.execute(
                """
                INSERT INTO news_sentiment (
                    time, source, article_url, canonical_url, source_name, title,
                    sentiment_score, sentiment_label
                )
                VALUES (
                    $1, 'sentiment_finbert', $2, NULL, 'CoinDesk', 'ETF headline', 0.42, 'neutral'
                )
                """,
                _utc("2026-03-22T09:15:00+06:00"),
                raw_identity,
            )

        await apply_repair_migrations(pool, tmp_path, "silver_news_identity_counts")

        async with pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT
                    time,
                    source,
                    url,
                    dedup_url,
                    sentiment_score,
                    sentiment_label,
                    sentiment_source,
                    source_count
                FROM v_news_deduped
                WHERE dedup_url = $1
                """,
                raw_identity,
            )

    assert dict(row) == {
        "time": _utc("2026-03-22T09:00:00+06:00"),
        "source": "news_rss_coindesk",
        "url": raw_identity,
        "dedup_url": raw_identity,
        "sentiment_score": 0.42,
        "sentiment_label": "neutral",
        "sentiment_source": "sentiment_finbert",
        "source_count": 1,
    }


@pytest.mark.asyncio
async def test_v_news_deduped_falls_back_to_latest_sentiment_for_empty_host_groups(
    migrated_db: dict[str, object],
) -> None:
    canonical_url = "https://canonical.example.com/articles/host-fallback"
    pool = migrated_db["pool"]

    async with pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO news (
                time, source, source_name, title, url, canonical_url, ingested_at
            )
            VALUES (
                $1, 'news_rss_coindesk', 'CoinDesk', 'Host fallback', $2, $3, $1
            )
            """,
            _utc("2026-03-22T10:00:00+06:00"),
            "https://www.example.com/articles/host-fallback?ref=rss",
            canonical_url,
        )
        await connection.execute(
            """
            INSERT INTO news_sentiment (
                time, source, article_url, canonical_url, source_name, title,
                sentiment_score, sentiment_label
            )
            VALUES
                ($1, 'sentiment_finbert', $2, $4, 'CoinDesk', 'Host fallback', 0.10, 'neutral'),
                ($3, 'sentiment_finbert', $5, $4, 'CoinDesk', 'Host fallback', 0.91, 'positive')
            """,
            _utc("2026-03-22T10:01:00+06:00"),
            "https://www.example.com/articles/host-fallback?model=finbert",
            _utc("2026-03-22T10:02:00+06:00"),
            canonical_url,
            "https://m.example.com/articles/host-fallback?model=finbert",
        )
        row = await connection.fetchrow(
            """
            SELECT dedup_url, sentiment_score, sentiment_label, sentiment_source, duplicate_count, source_count
            FROM v_news_deduped
            WHERE dedup_url = $1
            """,
            canonical_url,
        )

    assert dict(row) == {
        "dedup_url": canonical_url,
        "sentiment_score": 0.91,
        "sentiment_label": "positive",
        "sentiment_source": "sentiment_finbert",
        "duplicate_count": 0,
        "source_count": 1,
    }
