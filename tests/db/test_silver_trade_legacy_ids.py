from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

from ._migration_upgrade import (
    apply_repair_migrations,
    compress_tables,
    upgrade_db,
    utc,
)


@pytest.mark.asyncio
async def test_v_trades_excludes_binance_legacy_rows_without_stable_ids(
    tmp_path: Path,
) -> None:
    trade_time = utc("2026-03-22T06:00:00+00:00")

    async with upgrade_db(tmp_path) as pool:
        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO bn_agg_trades (
                    time, source, symbol, agg_trade_id, trade_id, price, qty, is_buyer_maker, ingested_at
                )
                VALUES
                    ($1, 'bn_ws_agg_trades', 'ETHUSDT', 51, NULL, 200.0, 1.0, FALSE, $6),
                    ($2, 'bn_ws_agg_trades', 'ETHUSDT', NULL, NULL, 201.0, 2.0, FALSE, $7),
                    ($3, 'bn_rest_agg_trades', 'ETHUSDT', NULL, NULL, 202.0, 3.0, TRUE, $8),
                    ($4, 'bn_rest_trades', 'ETHUSDT', NULL, 71, 203.0, 4.0, TRUE, $9),
                    ($5, 'bn_rest_trades', 'ETHUSDT', NULL, NULL, 204.0, 5.0, TRUE, $10)
                """,
                trade_time,
                trade_time + timedelta(milliseconds=1),
                trade_time + timedelta(milliseconds=2),
                trade_time + timedelta(milliseconds=3),
                trade_time + timedelta(milliseconds=4),
                utc("2026-03-22T06:10:00+00:00"),
                utc("2026-03-22T06:11:00+00:00"),
                utc("2026-03-22T06:12:00+00:00"),
                utc("2026-03-22T06:13:00+00:00"),
                utc("2026-03-22T06:14:00+00:00"),
            )

        await compress_tables(pool, "bn_agg_trades")
        await apply_repair_migrations(pool, tmp_path, "silver_trade_legacy_ids")

        async with pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT time, source, exchange, coin, side, price, size
                FROM v_trades
                WHERE exchange = 'binance'
                ORDER BY time
                """
            )

    assert [dict(row) for row in rows] == [
        {
            "time": trade_time,
            "source": "bn_ws_agg_trades",
            "exchange": "binance",
            "coin": "ETH",
            "side": "buy",
            "price": 200.0,
            "size": 1.0,
        },
    ]
