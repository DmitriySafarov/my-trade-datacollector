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
async def test_upgrade_repairs_trade_and_news_replay_history(tmp_path: Path) -> None:
    canonical_url = "https://canonical.example.com/articles/etf-headline"
    raw_identity = "https://example.com/articles/etf-headline"
    async with upgrade_db(tmp_path) as pool:
        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO hl_trades (
                    time, source, coin, tid, price, size, ingested_at
                )
                VALUES
                    ($1, 'hl_ws_trades', 'ETH', 11, 10.0, 1.0, $2),
                    ($3, 'hl_ws_trades', 'ETH', 11, 10.0, 1.0, $4),
                    ($5, 'hl_ws_trades', 'ETH', NULL, 10.0, 1.0, $5)
                """,
                utc("2026-03-22T00:01:00+00:00"),
                utc("2026-03-22T00:10:00+00:00"),
                utc("2026-03-22T00:01:00.001+00:00"),
                utc("2026-03-22T00:11:00+00:00"),
                utc("2026-03-22T00:12:00+00:00"),
            )
            await connection.execute(
                """
                INSERT INTO bn_agg_trades (time, source, symbol, agg_trade_id, trade_id, ingested_at)
                VALUES
                    ($1, 'bn_ws_agg_trades', 'ETHUSDT', 21, NULL, $2),
                    ($3, 'bn_ws_agg_trades', 'ETHUSDT', 21, NULL, $4),
                    ($5, 'bn_ws_agg_trades', 'ETHUSDT', NULL, NULL, $5),
                    ($6, 'bn_rest_trades', 'ETHUSDT', NULL, 23, $7),
                    ($8, 'bn_rest_trades', 'ETHUSDT', NULL, 23, $9)
                """,
                utc("2026-03-22T00:02:00+00:00"),
                utc("2026-03-22T00:20:00+00:00"),
                utc("2026-03-22T00:02:00.001+00:00"),
                utc("2026-03-22T00:21:00+00:00"),
                utc("2026-03-22T00:22:00+00:00"),
                utc("2026-03-22T00:03:00+00:00"),
                utc("2026-03-22T00:23:00+00:00"),
                utc("2026-03-22T00:03:00.001+00:00"),
                utc("2026-03-22T00:24:00+00:00"),
            )
            await connection.execute(
                """
                INSERT INTO news (
                    time, source, source_name, title, url, canonical_url, ingested_at
                )
                VALUES
                    ($1, 'news_rss_coindesk', 'CoinDesk', 'ETF headline', $2, NULL, $3),
                    ($4, 'news_rss_coindesk', 'CoinDesk', 'ETF headline', $5, $6, $7)
                """,
                utc("2026-03-22T00:30:00+00:00"),
                "https://example.com/articles/etf-headline/#updates",
                utc("2026-03-22T00:40:00+00:00"),
                utc("2026-03-22T00:31:00+00:00"),
                "https://example.com/articles/etf-headline/",
                canonical_url,
                utc("2026-03-22T00:41:00+00:00"),
            )
            await connection.execute(
                """
                INSERT INTO news_sentiment (
                    time, source, article_url, canonical_url, source_name, title,
                    sentiment_score, sentiment_label, ingested_at
                )
                VALUES
                    ($1, 'sentiment_finbert', $2, NULL, 'CoinDesk', 'ETF headline', 0.91, 'positive', $3),
                    ($4, 'sentiment_finbert', $5, $6, 'CoinDesk', 'ETF headline', 0.91, 'positive', $7)
                """,
                utc("2026-03-22T00:31:00+00:00"),
                "https://example.com/articles/etf-headline/#updates",
                utc("2026-03-22T00:42:00+00:00"),
                utc("2026-03-22T00:31:00.001+00:00"),
                "https://example.com/articles/etf-headline/",
                canonical_url,
                utc("2026-03-22T00:43:00+00:00"),
            )

        await compress_tables(
            pool, "hl_trades", "bn_agg_trades", "news", "news_sentiment"
        )
        await apply_repair_migrations(pool, tmp_path, "repair_trade_news")

        async with pool.acquire() as connection:
            counts = await connection.fetchrow(
                """
                SELECT
                    (SELECT count(*) FROM hl_trades WHERE source = 'hl_ws_trades' AND tid = 11) AS hl_trade_rows,
                    (SELECT count(*) FROM hl_trades WHERE tid IS NULL) AS hl_trade_null_rows,
                    (SELECT count(*) FROM hl_trade_registry WHERE source = 'hl_ws_trades' AND coin = 'ETH' AND tid = 11) AS hl_trade_registry_rows,
                    (SELECT count(*) FROM bn_agg_trades WHERE source = 'bn_ws_agg_trades' AND agg_trade_id = 21) AS bn_agg_rows,
                    (SELECT count(*) FROM bn_agg_trade_registry WHERE source = 'bn_ws_agg_trades' AND symbol = 'ETHUSDT' AND agg_trade_id = 21) AS bn_agg_registry_rows,
                    (SELECT count(*) FROM bn_agg_trades WHERE source = 'bn_rest_trades' AND trade_id = 23) AS bn_trade_rows,
                    (SELECT count(*) FROM bn_trade_registry WHERE source = 'bn_rest_trades' AND symbol = 'ETHUSDT' AND trade_id = 23) AS bn_trade_registry_rows,
                    (SELECT count(*) FROM bn_agg_trades WHERE source = 'bn_ws_agg_trades' AND agg_trade_id IS NULL) AS bn_bad_rows,
                    (SELECT count(*) FROM news WHERE source = 'news_rss_coindesk') AS news_rows,
                    (SELECT count(*) FROM news_dedup_registry WHERE source = 'news_rss_coindesk' AND dedup_url = $1) AS news_registry_rows,
                    (SELECT count(*) FROM news_sentiment WHERE source = 'sentiment_finbert') AS sentiment_rows,
                    (SELECT count(*) FROM news_sentiment_dedup_registry WHERE source = 'sentiment_finbert' AND dedup_url = $1) AS sentiment_registry_rows
                """,
                raw_identity,
            )
            first_seen_at = await connection.fetchval(
                """
                SELECT first_seen_at
                FROM news_provenance
                WHERE source = 'news_rss_coindesk'
                  AND dedup_url = $1
                """,
                raw_identity,
            )
            alias_primary = await connection.fetchval(
                """
                SELECT primary_dedup_url
                FROM news_dedup_aliases
                WHERE alias_url = $1
                """,
                canonical_url,
            )
            dedup_urls = {
                "news": {
                    row["dedup_url"]
                    for row in await connection.fetch(
                        """
                        SELECT dedup_url
                        FROM news
                        WHERE source = 'news_rss_coindesk'
                        """
                    )
                },
                "sentiment": {
                    row["dedup_url"]
                    for row in await connection.fetch(
                        """
                        SELECT dedup_url
                        FROM news_sentiment
                        WHERE source = 'sentiment_finbert'
                        """
                    )
                },
            }

    assert dict(counts) == {
        "hl_trade_rows": 2,
        "hl_trade_null_rows": 1,
        "hl_trade_registry_rows": 1,
        "bn_agg_rows": 2,
        "bn_agg_registry_rows": 1,
        "bn_trade_rows": 2,
        "bn_trade_registry_rows": 1,
        "bn_bad_rows": 1,
        "news_rows": 2,
        "news_registry_rows": 1,
        "sentiment_rows": 2,
        "sentiment_registry_rows": 1,
    }
    assert first_seen_at == utc("2026-03-22T00:40:00+00:00")
    assert alias_primary == raw_identity
    assert dedup_urls["news"] == {raw_identity, canonical_url}
    assert dedup_urls["sentiment"] == {raw_identity, canonical_url}
