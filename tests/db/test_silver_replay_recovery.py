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
async def test_v_orderbook_deduplicates_ws_replays_and_preserves_rest_polls(
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
                    ($3, 'hl_ws_l2book', 'ETH', 'snap-1', 99.0, 5.0, 100.0, 3.0, 1.0, $4)
                """,
                utc("2026-03-22T01:00:00+00:00"),
                utc("2026-03-22T01:10:00+00:00"),
                utc("2026-03-22T01:00:00.001+00:00"),
                utc("2026-03-22T01:11:00+00:00"),
            )
            await connection.execute(
                """
                INSERT INTO bn_depth (
                    time, source, symbol, final_update_id, bids, asks,
                    best_bid_price, best_ask_price, ingested_at
                )
                VALUES
                    ($1, 'bn_ws_depth', 'ETHUSDT', 101, '[["200","2"]]'::jsonb, '[["201","1"]]'::jsonb, 200.0, 201.0, $2),
                    ($3, 'bn_ws_depth', 'ETHUSDT', 101, '[["200","2"]]'::jsonb, '[["201","1"]]'::jsonb, 200.0, 201.0, $4),
                    ($5, 'bn_rest_depth', 'ETHUSDT', 777, '[["300","4"]]'::jsonb, '[["301","2"]]'::jsonb, 300.0, 301.0, $6),
                    ($7, 'bn_rest_depth', 'ETHUSDT', 777, '[["302","4"]]'::jsonb, '[["303","2"]]'::jsonb, 302.0, 303.0, $8)
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

        await compress_tables(pool, "hl_l2book", "bn_depth")
        await apply_repair_migrations(pool, tmp_path, "silver_orderbook_replays")

        async with pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT time, source, exchange, coin, spread
                FROM v_orderbook
                ORDER BY exchange, source, coin, time
                """
            )

    assert [dict(row) for row in rows] == [
        {
            "time": utc("2026-03-22T01:02:00+00:00"),
            "source": "bn_rest_depth",
            "exchange": "binance",
            "coin": "ETH",
            "spread": 1.0,
        },
        {
            "time": utc("2026-03-22T01:03:00+00:00"),
            "source": "bn_rest_depth",
            "exchange": "binance",
            "coin": "ETH",
            "spread": 1.0,
        },
        {
            "time": utc("2026-03-22T01:01:00+00:00"),
            "source": "bn_ws_depth",
            "exchange": "binance",
            "coin": "ETH",
            "spread": 1.0,
        },
        {
            "time": utc("2026-03-22T01:00:00+00:00"),
            "source": "hl_ws_l2book",
            "exchange": "hyperliquid",
            "coin": "ETH",
            "spread": 1.0,
        },
    ]


@pytest.mark.asyncio
async def test_v_funding_and_v_oi_deduplicate_asset_ctx_replays_and_surface_rest_recovery(
    tmp_path: Path,
) -> None:
    ws_time = utc("2026-03-22T02:00:00+00:00")
    replay_time = utc("2026-03-22T02:00:00.001+00:00")
    rest_only_time = utc("2026-03-22T02:05:00+00:00")

    async with upgrade_db(tmp_path) as pool:
        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO hl_asset_ctx (
                    time, source, coin, funding, open_interest, mark_price, event_hash, ingested_at
                )
                VALUES
                    ($1, 'hl_ws_asset_ctx', 'ETH', 0.01, 1500.0, 2000.0, 'ctx-1', $3),
                    ($2, 'hl_ws_asset_ctx', 'ETH', 0.01, 1500.0, 2000.0, 'ctx-1', $4)
                """,
                ws_time,
                replay_time,
                utc("2026-03-22T02:10:00+00:00"),
                utc("2026-03-22T02:11:00+00:00"),
            )
            await connection.execute(
                """
                INSERT INTO hl_rest_funding (time, source, coin, funding, ingested_at)
                VALUES
                    ($1, 'hl_rest_funding', 'ETH', 0.99, $3),
                    ($2, 'hl_rest_funding', 'ETH', 0.02, $4)
                """,
                ws_time,
                rest_only_time,
                utc("2026-03-22T02:12:00+00:00"),
                utc("2026-03-22T02:13:00+00:00"),
            )

        await compress_tables(pool, "hl_asset_ctx", "hl_rest_funding")
        await apply_repair_migrations(pool, tmp_path, "silver_asset_ctx_replays")

        async with pool.acquire() as connection:
            funding_rows = await connection.fetch(
                """
                SELECT time, source, exchange, coin, rate
                FROM v_funding
                WHERE exchange = 'hyperliquid'
                ORDER BY time, coin
                """
            )
            oi_rows = await connection.fetch(
                """
                SELECT time, source, exchange, coin, open_interest
                FROM v_oi
                WHERE exchange = 'hyperliquid'
                ORDER BY time, coin
                """
            )

    assert [dict(row) for row in funding_rows] == [
        {
            "time": ws_time,
            "source": "hl_ws_asset_ctx",
            "exchange": "hyperliquid",
            "coin": "ETH",
            "rate": 0.01,
        },
        {
            "time": rest_only_time,
            "source": "hl_rest_funding",
            "exchange": "hyperliquid",
            "coin": "ETH",
            "rate": 0.02,
        },
    ]
    assert [dict(row) for row in oi_rows] == [
        {
            "time": ws_time,
            "source": "hl_ws_asset_ctx",
            "exchange": "hyperliquid",
            "coin": "ETH",
            "open_interest": 1500.0,
        },
    ]
