from __future__ import annotations

from datetime import datetime, timezone

import pytest


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)


@pytest.mark.asyncio
async def test_v_candles_surfaces_hyperliquid_rest_recovery_and_prefers_ws(
    migrated_db: dict[str, object],
) -> None:
    ws_open_time = _utc("2026-03-22T10:00:00+06:00")
    ws_close_time = _utc("2026-03-22T10:01:00+06:00")
    rest_only_open_time = _utc("2026-03-22T10:05:00+06:00")
    rest_only_close_time = _utc("2026-03-22T10:06:00+06:00")
    pool = migrated_db["pool"]

    async with pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO hl_rest_candles (
                time, source, coin, interval, open_time, close_time,
                open, high, low, close, volume
            )
            VALUES
                ($1, 'hl_rest_candles', 'ETH', '1m', $1, $2, 10.0, 12.0, 9.0, 11.0, 50.0),
                ($3, 'hl_rest_candles', 'BTC', '1m', $3, $4, 20.0, 24.0, 19.0, 23.0, 60.0)
            """,
            ws_open_time,
            ws_close_time,
            rest_only_open_time,
            rest_only_close_time,
        )
        await connection.execute(
            """
            INSERT INTO hl_candles (
                time, source, coin, interval, open_time, close_time,
                open, high, low, close, volume, is_closed, event_hash
            )
            VALUES (
                $2, 'hl_ws_candles', 'ETH', '1m', $1, $2,
                10.0, 13.0, 9.0, 12.0, 55.0, TRUE, 'hl-candle-1'
            )
            """,
            ws_open_time,
            ws_close_time,
        )
        rows = await connection.fetch(
            """
            SELECT time, source, exchange, coin, interval, open, high, low, close, volume
            FROM v_candles
            WHERE exchange = 'hyperliquid'
            ORDER BY time, coin
            """
        )

    assert [dict(row) for row in rows] == [
        {
            "time": ws_open_time,
            "source": "hl_ws_candles",
            "exchange": "hyperliquid",
            "coin": "ETH",
            "interval": "1m",
            "open": 10.0,
            "high": 13.0,
            "low": 9.0,
            "close": 12.0,
            "volume": 55.0,
        },
        {
            "time": rest_only_open_time,
            "source": "hl_rest_candles",
            "exchange": "hyperliquid",
            "coin": "BTC",
            "interval": "1m",
            "open": 20.0,
            "high": 24.0,
            "low": 19.0,
            "close": 23.0,
            "volume": 60.0,
        },
    ]
