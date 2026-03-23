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
async def test_v_funding_preserves_distinct_ws_rows_sharing_a_timestamp(
    tmp_path: Path,
) -> None:
    shared_time = utc("2026-03-22T04:00:00+00:00")
    rest_only_time = utc("2026-03-22T04:05:00+00:00")

    async with upgrade_db(tmp_path) as pool:
        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO hl_asset_ctx (
                    time, source, coin, funding, open_interest, mark_price, event_hash, ingested_at
                )
                VALUES
                    ($1, 'hl_ws_asset_ctx', 'ETH', 0.01, 1500.0, 2000.0, 'ctx-1', $2),
                    ($1, 'hl_ws_asset_ctx', 'ETH', 0.02, 1501.0, 2001.0, 'ctx-2', $3)
                """,
                shared_time,
                utc("2026-03-22T04:10:00+00:00"),
                utc("2026-03-22T04:11:00+00:00"),
            )
            await connection.execute(
                """
                INSERT INTO hl_rest_funding (time, source, coin, funding, ingested_at)
                VALUES
                    ($1, 'hl_rest_funding', 'ETH', 0.99, $3),
                    ($2, 'hl_rest_funding', 'ETH', 0.03, $4)
                """,
                shared_time,
                rest_only_time,
                utc("2026-03-22T04:12:00+00:00"),
                utc("2026-03-22T04:13:00+00:00"),
            )

        await compress_tables(pool, "hl_asset_ctx", "hl_rest_funding")
        await apply_repair_migrations(
            pool, tmp_path, "silver_funding_timestamp_collisions"
        )

        async with pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT time, source, exchange, coin, rate
                FROM v_funding
                WHERE exchange = 'hyperliquid'
                ORDER BY time, source, rate
                """
            )

    assert [dict(row) for row in rows] == [
        {
            "time": shared_time,
            "source": "hl_ws_asset_ctx",
            "exchange": "hyperliquid",
            "coin": "ETH",
            "rate": 0.01,
        },
        {
            "time": shared_time,
            "source": "hl_ws_asset_ctx",
            "exchange": "hyperliquid",
            "coin": "ETH",
            "rate": 0.02,
        },
        {
            "time": rest_only_time,
            "source": "hl_rest_funding",
            "exchange": "hyperliquid",
            "coin": "ETH",
            "rate": 0.03,
        },
    ]
