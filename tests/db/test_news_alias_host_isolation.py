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
async def test_same_source_same_path_different_hosts_stay_distinct(
    migrated_db: dict[str, object],
) -> None:
    pool = migrated_db["pool"]

    async with pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO news (time, source, source_name, title, url, ingested_at)
            VALUES
                ($1, 'news_finnhub_general', 'Reuters', 'Macro headline', $2, $1),
                ($3, 'news_finnhub_general', 'Bloomberg', 'Macro headline', $4, $3)
            """,
            _utc("2026-03-22T10:00:00+00:00"),
            "https://www.reuters.com/markets/macro-headline",
            _utc("2026-03-22T10:01:00+00:00"),
            "https://www.bloomberg.com/markets/macro-headline",
        )
        await connection.execute(
            """
            INSERT INTO news_sentiment (
                time, source, article_url, source_name, title,
                sentiment_score, sentiment_label
            )
            VALUES
                ($1, 'sentiment_finbert', $2, 'Reuters', 'Macro headline', 0.3, 'neutral'),
                ($3, 'sentiment_finbert', $4, 'Bloomberg', 'Macro headline', -0.1, 'negative')
            """,
            _utc("2026-03-22T10:02:00+00:00"),
            "https://www.reuters.com/markets/macro-headline",
            _utc("2026-03-22T10:03:00+00:00"),
            "https://www.bloomberg.com/markets/macro-headline",
        )

        counts = await connection.fetchrow(
            """
            SELECT
                (SELECT count(*) FROM news WHERE source = 'news_finnhub_general') AS news_rows,
                (SELECT count(DISTINCT dedup_url) FROM news WHERE source = 'news_finnhub_general') AS news_dedup_values,
                (SELECT count(*) FROM news_dedup_registry WHERE source = 'news_finnhub_general') AS news_registry_rows,
                (SELECT count(*) FROM news_provenance WHERE source = 'news_finnhub_general') AS news_provenance_rows,
                (SELECT count(*) FROM news_sentiment WHERE source = 'sentiment_finbert') AS sentiment_rows,
                (SELECT count(DISTINCT dedup_url) FROM news_sentiment WHERE source = 'sentiment_finbert') AS sentiment_dedup_values,
                (SELECT count(*) FROM news_sentiment_dedup_registry WHERE source = 'sentiment_finbert') AS sentiment_registry_rows,
                (SELECT to_regclass('news_url_path_aliases')) AS legacy_path_alias_table
            """
        )

    assert dict(counts) == {
        "news_rows": 2,
        "news_dedup_values": 2,
        "news_registry_rows": 2,
        "news_provenance_rows": 2,
        "sentiment_rows": 2,
        "sentiment_dedup_values": 2,
        "sentiment_registry_rows": 2,
        "legacy_path_alias_table": None,
    }


@pytest.mark.asyncio
async def test_upgrade_repairs_keep_same_path_different_hosts_distinct(
    tmp_path: Path,
) -> None:
    async with upgrade_db(tmp_path) as pool:
        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO news (time, source, source_name, title, url, ingested_at)
                VALUES
                    ($1, 'news_finnhub_general', 'Reuters', 'Macro headline', $2, $1),
                    ($3, 'news_finnhub_general', 'Bloomberg', 'Macro headline', $4, $3)
                """,
                _utc("2026-03-22T11:00:00+00:00"),
                "https://www.reuters.com/markets/macro-headline",
                _utc("2026-03-22T11:01:00+00:00"),
                "https://www.bloomberg.com/markets/macro-headline",
            )
            await connection.execute(
                """
                INSERT INTO news_sentiment (
                    time, source, article_url, source_name, title,
                    sentiment_score, sentiment_label, ingested_at
                )
                VALUES
                    ($1, 'sentiment_finbert', $2, 'Reuters', 'Macro headline', 0.3, 'neutral', $1),
                    ($3, 'sentiment_finbert', $4, 'Bloomberg', 'Macro headline', -0.1, 'negative', $3)
                """,
                _utc("2026-03-22T11:02:00+00:00"),
                "https://www.reuters.com/markets/macro-headline",
                _utc("2026-03-22T11:03:00+00:00"),
                "https://www.bloomberg.com/markets/macro-headline",
            )

        await compress_tables(pool, "news", "news_sentiment")
        await apply_repair_migrations(pool, tmp_path, "repair_news_host_isolation")

        async with pool.acquire() as connection:
            counts = await connection.fetchrow(
                """
                SELECT
                    (SELECT count(*) FROM news WHERE source = 'news_finnhub_general') AS news_rows,
                    (SELECT count(DISTINCT dedup_url) FROM news WHERE source = 'news_finnhub_general') AS news_dedup_values,
                    (SELECT count(*) FROM news_dedup_registry WHERE source = 'news_finnhub_general') AS news_registry_rows,
                    (SELECT count(*) FROM news_provenance WHERE source = 'news_finnhub_general') AS news_provenance_rows,
                    (SELECT count(*) FROM news_sentiment WHERE source = 'sentiment_finbert') AS sentiment_rows,
                    (SELECT count(DISTINCT dedup_url) FROM news_sentiment WHERE source = 'sentiment_finbert') AS sentiment_dedup_values,
                    (SELECT count(*) FROM news_sentiment_dedup_registry WHERE source = 'sentiment_finbert') AS sentiment_registry_rows,
                    (SELECT to_regclass('news_url_path_aliases')) AS legacy_path_alias_table
                """
            )

    assert dict(counts) == {
        "news_rows": 2,
        "news_dedup_values": 2,
        "news_registry_rows": 2,
        "news_provenance_rows": 2,
        "sentiment_rows": 2,
        "sentiment_dedup_values": 2,
        "sentiment_registry_rows": 2,
        "legacy_path_alias_table": None,
    }
