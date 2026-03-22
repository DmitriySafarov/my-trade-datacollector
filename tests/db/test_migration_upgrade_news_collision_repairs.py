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
async def test_upgrade_repairs_quarantine_normalized_news_collisions(
    tmp_path: Path,
) -> None:
    event_time = utc("2026-03-22T00:30:00+00:00")

    async with upgrade_db(tmp_path) as pool:
        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO news (time, source, source_name, title, url, ingested_at)
                VALUES
                    ($1, 'news_rss_coindesk', 'CoinDesk', 'ETF headline', $2, $1),
                    ($1, 'news_rss_coindesk', 'CoinDesk', 'ETF headline', $3, $1)
                """,
                event_time,
                "https://example.com/articles/etf-headline/#updates",
                "https://example.com/articles/etf-headline/",
            )
            await connection.execute(
                """
                INSERT INTO news_sentiment (
                    time, source, article_url, source_name, title, sentiment_score, sentiment_label
                )
                VALUES
                    ($1, 'sentiment_finbert', $2, 'CoinDesk', 'ETF headline', 0.91, 'positive'),
                    ($1, 'sentiment_finbert', $3, 'CoinDesk', 'ETF headline', 0.91, 'positive')
                """,
                event_time,
                "https://example.com/articles/etf-headline/#updates",
                "https://example.com/articles/etf-headline/",
            )

        await compress_tables(pool, "news", "news_sentiment")
        await apply_repair_migrations(pool, tmp_path, "repair_news_collisions")

        async with pool.acquire() as connection:
            counts = await connection.fetchrow(
                """
                SELECT
                    (SELECT count(*) FROM news WHERE source = 'news_rss_coindesk') AS news_rows,
                    (SELECT count(DISTINCT dedup_url) FROM news WHERE source = 'news_rss_coindesk') AS news_dedup_values,
                    (SELECT count(*) FROM news_dedup_registry WHERE source = 'news_rss_coindesk') AS news_registry_rows,
                    (SELECT count(*) FROM news_sentiment WHERE source = 'sentiment_finbert') AS sentiment_rows,
                    (SELECT count(DISTINCT dedup_url) FROM news_sentiment WHERE source = 'sentiment_finbert') AS sentiment_dedup_values,
                    (SELECT count(*) FROM news_sentiment_dedup_registry WHERE source = 'sentiment_finbert') AS sentiment_registry_rows
                """
            )

    assert dict(counts) == {
        "news_rows": 2,
        "news_dedup_values": 1,
        "news_registry_rows": 1,
        "sentiment_rows": 2,
        "sentiment_dedup_values": 1,
        "sentiment_registry_rows": 1,
    }
