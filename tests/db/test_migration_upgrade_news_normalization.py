from __future__ import annotations

from pathlib import Path

import pytest

from ._migration_upgrade import (
    apply_repair_migrations,
    compress_tables,
    upgrade_db,
    utc,
)


@pytest.mark.asyncio
async def test_upgrade_repairs_preserve_normalized_news_duplicates(
    tmp_path: Path,
) -> None:
    normalized_url = "https://example.com/articles/etf-headline"

    async with upgrade_db(tmp_path) as pool:
        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO news (time, source, source_name, title, url, ingested_at)
                VALUES
                    ($1, 'news_rss_coindesk', 'CoinDesk', 'ETF headline', $2, $3),
                    ($1, 'news_rss_coindesk', 'CoinDesk', 'ETF headline', $4, $5)
                """,
                utc("2026-03-22T03:00:00+00:00"),
                "https://example.com/articles/etf-headline/#updates",
                utc("2026-03-22T03:10:00+00:00"),
                "https://example.com/articles/etf-headline/",
                utc("2026-03-22T03:11:00+00:00"),
            )
            await connection.execute(
                """
                INSERT INTO news_sentiment (
                    time, source, article_url, source_name, title, sentiment_score, sentiment_label, ingested_at
                )
                VALUES
                    ($1, 'sentiment_finbert', $2, 'CoinDesk', 'ETF headline', 0.91, 'positive', $3),
                    ($1, 'sentiment_finbert', $4, 'CoinDesk', 'ETF headline', 0.91, 'positive', $5)
                """,
                utc("2026-03-22T03:01:00+00:00"),
                "https://example.com/articles/etf-headline/#updates",
                utc("2026-03-22T03:12:00+00:00"),
                "https://example.com/articles/etf-headline/",
                utc("2026-03-22T03:13:00+00:00"),
            )

        await compress_tables(pool, "news", "news_sentiment")
        await apply_repair_migrations(pool, tmp_path, "repair_news_normalization")

        async with pool.acquire() as connection:
            counts = await connection.fetchrow(
                """
                SELECT
                    (SELECT count(*) FROM news WHERE source = 'news_rss_coindesk') AS news_rows,
                    (SELECT count(DISTINCT dedup_url) FROM news WHERE source = 'news_rss_coindesk') AS news_dedup_values,
                    (SELECT count(*) FROM news_dedup_registry WHERE source = 'news_rss_coindesk' AND dedup_url = $1) AS news_registry_rows,
                    (SELECT count(*) FROM news_sentiment WHERE source = 'sentiment_finbert') AS sentiment_rows,
                    (SELECT count(DISTINCT dedup_url) FROM news_sentiment WHERE source = 'sentiment_finbert') AS sentiment_dedup_values,
                    (SELECT count(*) FROM news_sentiment_dedup_registry WHERE source = 'sentiment_finbert' AND dedup_url = $1) AS sentiment_registry_rows
                """,
                normalized_url,
            )

    assert dict(counts) == {
        "news_rows": 2,
        "news_dedup_values": 1,
        "news_registry_rows": 1,
        "sentiment_rows": 2,
        "sentiment_dedup_values": 1,
        "sentiment_registry_rows": 1,
    }
