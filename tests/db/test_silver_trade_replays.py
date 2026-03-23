from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from ._migration_upgrade import apply_repair_migrations, upgrade_db


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)


@pytest.mark.asyncio
async def test_v_trades_deduplicates_historical_replay_rows(
    tmp_path: Path,
) -> None:
    trade_time = _utc("2026-03-22T10:00:00+06:00")
    replay_time = trade_time + timedelta(milliseconds=1)

    async with upgrade_db(tmp_path) as pool:
        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO hl_trades (time, source, coin, side, price, size, tid, ingested_at)
                VALUES
                    ($1, 'hl_ws_trades', 'ETH', 'B', 100.5, 2.0, 11, $3),
                    ($2, 'hl_ws_trades', 'ETH', 'B', 100.5, 2.0, 11, $4)
                """,
                trade_time,
                replay_time,
                trade_time + timedelta(minutes=10),
                trade_time + timedelta(minutes=11),
            )
            await connection.execute(
                """
                INSERT INTO bn_agg_trades (
                    time, source, symbol, agg_trade_id, price, qty, is_buyer_maker, ingested_at
                )
                VALUES
                    ($1, 'bn_ws_agg_trades', 'ETHUSDT', 21, 200.5, 3.0, FALSE, $3),
                    ($2, 'bn_ws_agg_trades', 'ETHUSDT', 21, 200.5, 3.0, FALSE, $4)
                """,
                trade_time,
                replay_time,
                trade_time + timedelta(minutes=20),
                trade_time + timedelta(minutes=21),
            )

        await apply_repair_migrations(pool, tmp_path, "silver_trade_replays")

        async with pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT time, source, exchange, coin, side, price, size
                FROM v_trades
                ORDER BY exchange, coin, time
                """
            )

    assert len(rows) == 2
    assert [dict(row) for row in rows] == [
        {
            "time": trade_time,
            "source": "bn_ws_agg_trades",
            "exchange": "binance",
            "coin": "ETH",
            "side": "buy",
            "price": 200.5,
            "size": 3.0,
        },
        {
            "time": trade_time,
            "source": "hl_ws_trades",
            "exchange": "hyperliquid",
            "coin": "ETH",
            "side": "buy",
            "price": 100.5,
            "size": 2.0,
        },
    ]


@pytest.mark.asyncio
async def test_v_trades_excludes_legacy_binance_rows_without_ids(
    tmp_path: Path,
) -> None:
    trade_time = _utc("2026-03-22T11:00:00+06:00")

    async with upgrade_db(tmp_path) as pool:
        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO bn_agg_trades (
                    time, source, symbol, agg_trade_id, trade_id, price, qty, is_buyer_maker, ingested_at
                )
                VALUES
                    ($1, 'bn_ws_agg_trades', 'ETHUSDT', NULL, NULL, 200.0, 1.0, FALSE, $3),
                    ($2, 'bn_rest_trades', 'ETHUSDT', NULL, NULL, 201.0, 2.0, TRUE, $4),
                    ($1, 'bn_ws_agg_trades', 'ETHUSDT', 21, 21, 202.0, 3.0, FALSE, $5),
                    ($2, 'bn_rest_trades', 'ETHUSDT', NULL, 99, 203.0, 4.0, TRUE, $6)
                """,
                trade_time,
                trade_time + timedelta(seconds=1),
                trade_time + timedelta(minutes=10),
                trade_time + timedelta(minutes=11),
                trade_time + timedelta(minutes=12),
                trade_time + timedelta(minutes=13),
            )

        await apply_repair_migrations(pool, tmp_path, "silver_trade_null_ids")

        async with pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT time, source, exchange, coin, side, price, size
                FROM v_trades
                WHERE exchange = 'binance'
                ORDER BY source, time
                """
            )

    assert [dict(row) for row in rows] == [
        {
            "time": trade_time,
            "source": "bn_ws_agg_trades",
            "exchange": "binance",
            "coin": "ETH",
            "side": "buy",
            "price": 202.0,
            "size": 3.0,
        },
    ]
