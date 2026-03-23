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
async def test_silver_views_quarantine_missing_ws_replay_keys_on_upgrade(
    tmp_path: Path,
) -> None:
    async with upgrade_db(tmp_path) as pool:
        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO hl_l2book (
                    time, source, coin, snapshot_hash, best_bid_price, best_bid_size,
                    best_ask_price, best_ask_size, spread, ingested_at
                )
                VALUES
                    ($1, 'hl_ws_l2book', 'ETH', NULL, 99.0, 5.0, 100.0, 3.0, 1.0, $3),
                    ($2, 'hl_ws_l2book', 'ETH', NULL, 99.0, 5.0, 100.0, 3.0, 1.0, $4)
                """,
                utc("2026-03-22T03:00:00+00:00"),
                utc("2026-03-22T03:00:00.001+00:00"),
                utc("2026-03-22T03:10:00+00:00"),
                utc("2026-03-22T03:11:00+00:00"),
            )
            await connection.execute(
                """
                INSERT INTO bn_depth (
                    time, source, symbol, final_update_id, last_update_id, snapshot_hash,
                    bids, asks, best_bid_price, best_ask_price, ingested_at
                )
                VALUES
                    ($1, 'bn_ws_depth', 'ETHUSDT', NULL, NULL, NULL, '[["200","2"]]'::jsonb, '[["201","1"]]'::jsonb, 200.0, 201.0, $3),
                    ($2, 'bn_ws_depth', 'ETHUSDT', NULL, NULL, NULL, '[["200","2"]]'::jsonb, '[["201","1"]]'::jsonb, 200.0, 201.0, $4)
                """,
                utc("2026-03-22T03:01:00+00:00"),
                utc("2026-03-22T03:01:00.001+00:00"),
                utc("2026-03-22T03:12:00+00:00"),
                utc("2026-03-22T03:13:00+00:00"),
            )
            await connection.execute(
                """
                INSERT INTO hl_asset_ctx (
                    time, source, coin, funding, open_interest, mark_price, event_hash, ingested_at
                )
                VALUES
                    ($1, 'hl_ws_asset_ctx', 'ETH', 0.01, 1500.0, 2000.0, NULL, $3),
                    ($2, 'hl_ws_asset_ctx', 'ETH', 0.01, 1500.0, 2000.0, NULL, $4)
                """,
                utc("2026-03-22T03:02:00+00:00"),
                utc("2026-03-22T03:02:00.001+00:00"),
                utc("2026-03-22T03:14:00+00:00"),
                utc("2026-03-22T03:15:00+00:00"),
            )
            await connection.execute(
                """
                INSERT INTO hl_rest_funding (time, source, coin, funding, ingested_at)
                VALUES ($1, 'hl_rest_funding', 'ETH', 0.02, $2)
                """,
                utc("2026-03-22T03:03:00+00:00"),
                utc("2026-03-22T03:16:00+00:00"),
            )

        await compress_tables(
            pool, "hl_l2book", "bn_depth", "hl_asset_ctx", "hl_rest_funding"
        )
        await apply_repair_migrations(pool, tmp_path, "silver_missing_replay_keys")

        async with pool.acquire() as connection:
            orderbook_count = await connection.fetchval(
                "SELECT count(*) FROM v_orderbook"
            )
            funding_rows = await connection.fetch(
                """
                SELECT time, source, exchange, coin, rate
                FROM v_funding
                WHERE exchange = 'hyperliquid'
                ORDER BY time, coin
                """,
            )
            oi_count = await connection.fetchval(
                """
                SELECT count(*)
                FROM v_oi
                WHERE exchange = 'hyperliquid'
                """,
            )

    assert orderbook_count == 0
    assert [dict(row) for row in funding_rows] == [
        {
            "time": utc("2026-03-22T03:03:00+00:00"),
            "source": "hl_rest_funding",
            "exchange": "hyperliquid",
            "coin": "ETH",
            "rate": 0.02,
        },
    ]
    assert oi_count == 0
