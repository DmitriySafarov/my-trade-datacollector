from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)


@pytest.mark.asyncio
async def test_v_trades_v_funding_and_v_oi_normalize_and_filter_rows(
    migrated_db: dict[str, object],
) -> None:
    trade_time = _utc("2026-03-22T10:00:00+06:00")
    rest_agg_time = trade_time + timedelta(seconds=1)
    rest_trade_time = trade_time + timedelta(seconds=2)
    funding_time = _utc("2026-03-22T10:05:00+06:00")
    pool = migrated_db["pool"]

    async with pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO hl_trades (time, source, coin, side, price, size, tid)
            VALUES ($1, 'hl_ws_trades', 'ETH', 'B', 100.5, 2.0, 11)
            """,
            trade_time,
        )
        await connection.execute(
            """
            INSERT INTO bn_agg_trades (
                time, source, symbol, agg_trade_id, trade_id, price, qty, is_buyer_maker
            )
            VALUES
                ($1, 'bn_ws_agg_trades', 'BTCUSDT', 21, 21, 200.5, 3.0, FALSE),
                ($2, 'bn_rest_agg_trades', 'BTCUSDT', 21, 21, 200.5, 3.0, TRUE),
                ($3, 'bn_rest_trades', 'BTCUSDT', NULL, 99, 999.0, 9.0, TRUE)
            """,
            trade_time,
            rest_agg_time,
            rest_trade_time,
        )
        await connection.execute(
            """
            INSERT INTO hl_asset_ctx (
                time, source, coin, funding, open_interest, event_hash
            )
            VALUES ($1, 'hl_ws_asset_ctx', 'ETH', 0.01, 1500.0, 'ctx-1')
            """,
            funding_time,
        )
        await connection.execute(
            """
            INSERT INTO bn_rest_funding (time, source, symbol, funding_rate)
            VALUES ($1, 'bn_rest_funding', 'BTCUSDT', 0.02)
            """,
            funding_time,
        )
        await connection.execute(
            """
            INSERT INTO bn_rest_oi (time, source, symbol, open_interest)
            VALUES ($1, 'bn_rest_oi', 'BTCUSDT', 3200.0)
            """,
            funding_time,
        )

        trades = await connection.fetch(
            """
            SELECT time, source, exchange, coin, side, price, size
            FROM v_trades
            ORDER BY exchange, source, coin
            """,
        )
        funding = await connection.fetch(
            "SELECT time, source, exchange, coin, rate FROM v_funding ORDER BY exchange, coin",
        )
        oi_rows = await connection.fetch(
            """
            SELECT time, source, exchange, coin, open_interest
            FROM v_oi
            ORDER BY exchange, coin
            """,
        )

    assert [dict(row) for row in trades] == [
        {
            "time": trade_time,
            "source": "bn_ws_agg_trades",
            "exchange": "binance",
            "coin": "BTC",
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
    assert [dict(row) for row in funding] == [
        {
            "time": funding_time,
            "source": "bn_rest_funding",
            "exchange": "binance",
            "coin": "BTC",
            "rate": 0.02,
        },
        {
            "time": funding_time,
            "source": "hl_ws_asset_ctx",
            "exchange": "hyperliquid",
            "coin": "ETH",
            "rate": 0.01,
        },
    ]
    assert [dict(row) for row in oi_rows] == [
        {
            "time": funding_time,
            "source": "bn_rest_oi",
            "exchange": "binance",
            "coin": "BTC",
            "open_interest": 3200.0,
        },
        {
            "time": funding_time,
            "source": "hl_ws_asset_ctx",
            "exchange": "hyperliquid",
            "coin": "ETH",
            "open_interest": 1500.0,
        },
    ]
