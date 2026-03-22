from __future__ import annotations

from datetime import datetime, timezone

import pytest


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)


@pytest.mark.asyncio
async def test_news_rows_land_once_per_source_and_track_provenance(
    migrated_db: dict[str, object],
) -> None:
    event_time = _utc("2026-03-22T06:00:00+06:00")
    replay_time = _utc("2026-03-22T06:05:00+06:00")
    canonical_url = "https://canonical.example.com/articles/etf-headline"
    pool = migrated_db["pool"]

    async with pool.acquire() as connection:
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
            event_time,
            "https://example.com/articles/etf-headline?utm_source=rss",
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
            VALUES ($1, 'news_rss_coindesk', 'CoinDesk', 'ETF headline', $2, $3, $1)
            ON CONFLICT DO NOTHING
            """,
            replay_time,
            "https://m.example.com/articles/etf-headline/",
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
            replay_time,
            "https://api.example.com/articles/etf-headline?ref=finnhub",
            canonical_url,
        )

        rows = await connection.fetch(
            """
            SELECT source, url, dedup_url, time
            FROM news
            WHERE dedup_url = $1
            ORDER BY source, time
            """,
            canonical_url,
        )
        row_count = await connection.fetchval(
            "SELECT count(*) FROM news WHERE dedup_url = $1",
            canonical_url,
        )
        sources = {
            record["source"]
            for record in await connection.fetch(
                """
                SELECT source
                FROM news_provenance
                WHERE dedup_url = $1
                """,
                canonical_url,
            )
        }
        first_seen_at = await connection.fetchval(
            """
            SELECT first_seen_at
            FROM news_provenance
            WHERE dedup_url = $1
              AND source = 'news_rss_coindesk'
            """,
            canonical_url,
        )

    assert row_count == 3
    assert sources == {"news_rss_coindesk", "news_finnhub"}
    assert [row["source"] for row in rows] == [
        "news_finnhub",
        "news_rss_coindesk",
        "news_rss_coindesk",
    ]
    assert all(row["dedup_url"] == canonical_url for row in rows)
    assert [row["url"] for row in rows] == [
        "https://api.example.com/articles/etf-headline?ref=finnhub",
        "https://example.com/articles/etf-headline?utm_source=rss",
        "https://m.example.com/articles/etf-headline/",
    ]
    assert {row["time"] for row in rows} == {event_time, replay_time}
    assert first_seen_at == event_time


@pytest.mark.asyncio
async def test_news_dedup_url_matches_normalized_registry_identity_without_canonical_url(
    migrated_db: dict[str, object],
) -> None:
    event_time = _utc("2026-03-22T07:00:00+06:00")
    replay_time = _utc("2026-03-22T07:05:00+06:00")
    normalized_url = "https://example.com/articles/etf-headline"
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
            INSERT INTO news (time, source, source_name, title, url, ingested_at)
            VALUES ($1, 'news_rss_coindesk', 'CoinDesk', 'ETF headline', $2, $1)
            ON CONFLICT DO NOTHING
            """,
            replay_time,
            "https://example.com/articles/etf-headline/",
        )

        row = await connection.fetchrow(
            """
            SELECT source, url, dedup_url, time
            FROM news
            WHERE dedup_url = $1
            """,
            normalized_url,
        )
        row_count = await connection.fetchval(
            "SELECT count(*) FROM news WHERE dedup_url = $1",
            normalized_url,
        )
        provenance_count = await connection.fetchval(
            """
            SELECT count(*)
            FROM news_provenance
            WHERE source = 'news_rss_coindesk'
              AND dedup_url = $1
            """,
            normalized_url,
        )

    assert row_count == 1
    assert provenance_count == 1
    assert row["source"] == "news_rss_coindesk"
    assert row["url"] == "https://example.com/articles/etf-headline/#updates"
    assert row["dedup_url"] == normalized_url
    assert row["time"] == event_time
