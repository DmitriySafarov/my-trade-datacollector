from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from ._migration_upgrade import apply_repair_migrations, compress_tables, upgrade_db


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)


@pytest.mark.asyncio
async def test_same_source_canonical_claim_keeps_later_host_distinct(
    migrated_db: dict[str, object],
) -> None:
    canonical_url = "https://canonical.example.com/articles/etf-headline"
    second_raw = "https://m.example.com/articles/etf-headline?ref=mobile"
    pool = migrated_db["pool"]

    async with pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO news (
                time, source, source_name, title, url, canonical_url, ingested_at
            )
            VALUES
                ($1, 'news_rss_coindesk', 'CoinDesk', 'ETF headline', $2, $3, $1),
                ($4, 'news_rss_coindesk', 'CoinDesk', 'ETF headline', $5, $3, $4)
            """,
            _utc("2026-03-22T08:00:00+06:00"),
            "https://www.example.com/articles/etf-headline",
            canonical_url,
            _utc("2026-03-22T08:05:00+06:00"),
            second_raw,
        )
        row_count = await connection.fetchval(
            "SELECT count(*) FROM news WHERE source = 'news_rss_coindesk'",
        )
        registry_rows = await connection.fetch(
            """
            SELECT dedup_url, raw_host
            FROM news_dedup_registry
            WHERE source = 'news_rss_coindesk'
            ORDER BY raw_host
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

    assert row_count == 2
    assert [(row["dedup_url"], row["raw_host"]) for row in registry_rows] == [
        (canonical_url, "m.example.com"),
        (canonical_url, "www.example.com"),
    ]
    assert alias_primary == canonical_url


@pytest.mark.asyncio
async def test_upgrade_repairs_keep_canonical_claimed_hosts_distinct(
    tmp_path: Path,
) -> None:
    canonical_url = "https://canonical.example.com/articles/etf-headline"
    second_raw = "https://m.example.com/articles/etf-headline?ref=mobile"

    async with upgrade_db(tmp_path) as pool:
        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO news (
                    time, source, source_name, title, url, canonical_url, ingested_at
                )
                VALUES
                    ($1, 'news_rss_coindesk', 'CoinDesk', 'ETF headline', $2, $3, $1),
                    ($4, 'news_rss_coindesk', 'CoinDesk', 'ETF headline', $5, $3, $4)
                """,
                _utc("2026-03-22T09:00:00+06:00"),
                "https://www.example.com/articles/etf-headline",
                canonical_url,
                _utc("2026-03-22T09:05:00+06:00"),
                second_raw,
            )

        await compress_tables(pool, "news")
        await apply_repair_migrations(pool, tmp_path, "repair_news_claimed_hosts")

        async with pool.acquire() as connection:
            row_count = await connection.fetchval(
                "SELECT count(*) FROM news WHERE source = 'news_rss_coindesk'",
            )
            registry_rows = await connection.fetch(
                """
                SELECT dedup_url, raw_host
                FROM news_dedup_registry
                WHERE source = 'news_rss_coindesk'
                ORDER BY raw_host
                """,
            )
            provenance_urls = [
                row["dedup_url"]
                for row in await connection.fetch(
                    """
                    SELECT dedup_url
                    FROM news_provenance
                    WHERE source = 'news_rss_coindesk'
                    ORDER BY dedup_url
                    """,
                )
            ]

    assert row_count == 2
    assert [(row["dedup_url"], row["raw_host"]) for row in registry_rows] == [
        (canonical_url, "m.example.com"),
        (canonical_url, "www.example.com"),
    ]
    assert provenance_urls == [canonical_url]
