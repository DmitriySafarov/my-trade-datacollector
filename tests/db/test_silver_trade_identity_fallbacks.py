from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from ._migration_upgrade import apply_repair_migrations, upgrade_db


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)


@pytest.mark.asyncio
async def test_v_trades_uses_binance_trade_id_fallback_without_collapsing_null_agg_ids(
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
                    ($1, 'bn_ws_agg_trades', 'BTCUSDT', NULL, 301, 500.0, 1.0, FALSE, $4),
                    ($2, 'bn_ws_agg_trades', 'BTCUSDT', NULL, 302, 501.0, 2.0, TRUE, $5),
                    ($3, 'bn_ws_agg_trades', 'BTCUSDT', NULL, NULL, 999.0, 9.0, FALSE, $6)
                """,
                trade_time,
                trade_time + timedelta(milliseconds=1),
                trade_time + timedelta(milliseconds=2),
                trade_time + timedelta(minutes=10),
                trade_time + timedelta(minutes=11),
                trade_time + timedelta(minutes=12),
            )

        await apply_repair_migrations(pool, tmp_path, "silver_trade_id_fallback")

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
            "coin": "BTC",
            "side": "buy",
            "price": 500.0,
            "size": 1.0,
        },
        {
            "time": trade_time + timedelta(milliseconds=1),
            "source": "bn_ws_agg_trades",
            "exchange": "binance",
            "coin": "BTC",
            "side": "sell",
            "price": 501.0,
            "size": 2.0,
        },
    ]
