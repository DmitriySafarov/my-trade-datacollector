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
async def test_upgrade_repairs_ws_replay_registry_backfill(tmp_path: Path) -> None:
    async with upgrade_db(tmp_path) as pool:
        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO hl_l2book (time, source, coin, snapshot_hash, ingested_at)
                VALUES
                    ($1, 'hl_ws_l2book', 'ETH', 'snap-1', $2),
                    ($3, 'hl_ws_l2book', 'ETH', 'snap-1', $4)
                """,
                utc("2026-03-22T01:00:00+00:00"),
                utc("2026-03-22T01:10:00+00:00"),
                utc("2026-03-22T01:00:00.001+00:00"),
                utc("2026-03-22T01:11:00+00:00"),
            )
            await connection.execute(
                """
                INSERT INTO bn_depth (time, source, symbol, final_update_id, ingested_at)
                VALUES
                    ($1, 'bn_ws_depth', 'ETHUSDT', 101, $2),
                    ($3, 'bn_ws_depth', 'ETHUSDT', 101, $4),
                    ($5, 'bn_rest_depth', 'ETHUSDT', 777, $6),
                    ($7, 'bn_rest_depth', 'ETHUSDT', 777, $8)
                """,
                utc("2026-03-22T01:01:00+00:00"),
                utc("2026-03-22T01:20:00+00:00"),
                utc("2026-03-22T01:01:00.001+00:00"),
                utc("2026-03-22T01:21:00+00:00"),
                utc("2026-03-22T01:02:00+00:00"),
                utc("2026-03-22T01:22:00+00:00"),
                utc("2026-03-22T01:03:00+00:00"),
                utc("2026-03-22T01:23:00+00:00"),
            )
            await connection.execute(
                """
                INSERT INTO bn_tickers (time, source, symbol, update_id, event_hash, ingested_at)
                VALUES
                    ($1, 'bn_ws_book_ticker', 'ETHUSDT', 301, NULL, $2),
                    ($3, 'bn_ws_book_ticker', 'ETHUSDT', 301, NULL, $4),
                    ($5, 'bn_ws_ticker', 'ETHUSDT', NULL, 'ticker-1', $6),
                    ($7, 'bn_ws_ticker', 'ETHUSDT', NULL, 'ticker-1', $8)
                """,
                utc("2026-03-22T01:04:00+00:00"),
                utc("2026-03-22T01:24:00+00:00"),
                utc("2026-03-22T01:04:00.001+00:00"),
                utc("2026-03-22T01:25:00+00:00"),
                utc("2026-03-22T01:05:00+00:00"),
                utc("2026-03-22T01:26:00+00:00"),
                utc("2026-03-22T01:05:00.001+00:00"),
                utc("2026-03-22T01:27:00+00:00"),
            )

        await compress_tables(pool, "hl_l2book", "bn_depth", "bn_tickers")
        await apply_repair_migrations(pool, tmp_path, "repair_ws")

        async with pool.acquire() as connection:
            counts = await connection.fetchrow(
                """
                SELECT
                    (SELECT count(*) FROM hl_l2book WHERE snapshot_hash = 'snap-1') AS hl_l2book_rows,
                    (SELECT count(*) FROM bronze_replay_registry WHERE table_name = 'hl_l2book' AND source = 'hl_ws_l2book' AND dedup_key = 'snap-1') AS hl_l2book_registry_rows,
                    (SELECT count(*) FROM bn_depth WHERE source = 'bn_ws_depth' AND final_update_id = 101) AS bn_ws_depth_rows,
                    (SELECT count(*) FROM bronze_replay_registry WHERE table_name = 'bn_depth' AND source = 'bn_ws_depth' AND dedup_key = '101') AS bn_ws_depth_registry_rows,
                    (SELECT count(*) FROM bn_depth WHERE source = 'bn_rest_depth' AND final_update_id = 777) AS bn_rest_depth_rows,
                    (SELECT count(*) FROM bronze_replay_registry WHERE table_name = 'bn_depth' AND source = 'bn_rest_depth') AS rest_depth_registry_rows,
                    (SELECT count(*) FROM bn_tickers WHERE source = 'bn_ws_book_ticker' AND update_id = 301) AS book_ticker_rows,
                    (SELECT count(*) FROM bronze_replay_registry WHERE table_name = 'bn_tickers' AND source = 'bn_ws_book_ticker' AND dedup_key = '301') AS book_ticker_registry_rows,
                    (SELECT count(*) FROM bn_tickers WHERE source = 'bn_ws_ticker' AND event_hash = 'ticker-1') AS ticker_rows,
                    (SELECT count(*) FROM bronze_replay_registry WHERE table_name = 'bn_tickers' AND source = 'bn_ws_ticker' AND dedup_key = 'ticker-1') AS ticker_registry_rows
                """
            )

    assert dict(counts) == {
        "hl_l2book_rows": 2,
        "hl_l2book_registry_rows": 1,
        "bn_ws_depth_rows": 2,
        "bn_ws_depth_registry_rows": 1,
        "bn_rest_depth_rows": 2,
        "rest_depth_registry_rows": 0,
        "book_ticker_rows": 2,
        "book_ticker_registry_rows": 1,
        "ticker_rows": 2,
        "ticker_registry_rows": 1,
    }
