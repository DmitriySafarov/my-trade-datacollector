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
async def test_silver_views_drop_missing_key_ws_legacy_rows_after_repairs(
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
                    ($1, 'hl_ws_l2book', 'ETH', 'snap-1', 99.0, 5.0, 100.0, 3.0, 1.0, $2),
                    ($3, 'hl_ws_l2book', 'ETH', NULL, 101.0, 6.0, 102.0, 4.0, 1.0, $4),
                    ($5, 'hl_ws_l2book', 'ETH', NULL, 101.0, 6.0, 102.0, 4.0, 1.0, $6)
                """,
                utc("2026-03-22T04:00:00+00:00"),
                utc("2026-03-22T04:10:00+00:00"),
                utc("2026-03-22T04:01:00+00:00"),
                utc("2026-03-22T04:11:00+00:00"),
                utc("2026-03-22T04:01:00.001+00:00"),
                utc("2026-03-22T04:12:00+00:00"),
            )
            await connection.execute(
                """
                INSERT INTO bn_depth (
                    time, source, symbol, final_update_id, bids, asks,
                    best_bid_price, best_ask_price, ingested_at
                )
                VALUES
                    ($1, 'bn_ws_depth', 'ETHUSDT', 201, '[["200","2"]]'::jsonb, '[["201","1"]]'::jsonb, 200.0, 201.0, $2),
                    ($3, 'bn_ws_depth', 'ETHUSDT', NULL, '[["202","2"]]'::jsonb, '[["203","1"]]'::jsonb, 202.0, 203.0, $4),
                    ($5, 'bn_ws_depth', 'ETHUSDT', NULL, '[["202","2"]]'::jsonb, '[["203","1"]]'::jsonb, 202.0, 203.0, $6)
                """,
                utc("2026-03-22T04:02:00+00:00"),
                utc("2026-03-22T04:13:00+00:00"),
                utc("2026-03-22T04:03:00+00:00"),
                utc("2026-03-22T04:14:00+00:00"),
                utc("2026-03-22T04:03:00.001+00:00"),
                utc("2026-03-22T04:15:00+00:00"),
            )
            await connection.execute(
                """
                INSERT INTO hl_asset_ctx (
                    time, source, coin, funding, open_interest, mark_price, event_hash, ingested_at
                )
                VALUES
                    ($1, 'hl_ws_asset_ctx', 'ETH', 0.01, 1500.0, 2000.0, 'ctx-1', $2),
                    ($3, 'hl_ws_asset_ctx', 'ETH', 0.02, 1600.0, 2100.0, NULL, $4),
                    ($5, 'hl_ws_asset_ctx', 'ETH', 0.02, 1600.0, 2100.0, NULL, $6)
                """,
                utc("2026-03-22T04:04:00+00:00"),
                utc("2026-03-22T04:16:00+00:00"),
                utc("2026-03-22T04:05:00+00:00"),
                utc("2026-03-22T04:17:00+00:00"),
                utc("2026-03-22T04:05:00.001+00:00"),
                utc("2026-03-22T04:18:00+00:00"),
            )

        await compress_tables(pool, "hl_l2book", "bn_depth", "hl_asset_ctx")
        await apply_repair_migrations(pool, tmp_path, "silver_missing_key_repairs")

        async with pool.acquire() as connection:
            orderbook_rows = await connection.fetch(
                """
                SELECT time, source, exchange, coin
                FROM v_orderbook
                WHERE coin = 'ETH'
                ORDER BY exchange, source, time
                """
            )
            funding_rows = await connection.fetch(
                """
                SELECT time, source, exchange, coin, rate
                FROM v_funding
                WHERE exchange = 'hyperliquid'
                ORDER BY time
                """
            )
            oi_rows = await connection.fetch(
                """
                SELECT time, source, exchange, coin, open_interest
                FROM v_oi
                WHERE exchange = 'hyperliquid'
                ORDER BY time
                """
            )

    assert [dict(row) for row in orderbook_rows] == [
        {
            "time": utc("2026-03-22T04:02:00+00:00"),
            "source": "bn_ws_depth",
            "exchange": "binance",
            "coin": "ETH",
        },
        {
            "time": utc("2026-03-22T04:00:00+00:00"),
            "source": "hl_ws_l2book",
            "exchange": "hyperliquid",
            "coin": "ETH",
        },
    ]
    assert [dict(row) for row in funding_rows] == [
        {
            "time": utc("2026-03-22T04:04:00+00:00"),
            "source": "hl_ws_asset_ctx",
            "exchange": "hyperliquid",
            "coin": "ETH",
            "rate": 0.01,
        },
    ]
    assert [dict(row) for row in oi_rows] == [
        {
            "time": utc("2026-03-22T04:04:00+00:00"),
            "source": "hl_ws_asset_ctx",
            "exchange": "hyperliquid",
            "coin": "ETH",
            "open_interest": 1500.0,
        },
    ]
