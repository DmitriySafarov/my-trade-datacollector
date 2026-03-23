from __future__ import annotations

from datetime import datetime, timezone

import pytest


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)


@pytest.mark.asyncio
async def test_v_orderbook_and_v_candles_normalize_binance_and_hyperliquid_rows(
    migrated_db: dict[str, object],
) -> None:
    book_time = _utc("2026-03-22T10:10:00+06:00")
    candle_open_time = _utc("2026-03-22T10:15:00+06:00")
    candle_close_time = _utc("2026-03-22T10:16:00+06:00")
    pool = migrated_db["pool"]

    async with pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO hl_l2book (
                time, source, coin, snapshot_hash, best_bid_price, best_bid_size,
                best_ask_price, best_ask_size, spread
            )
            VALUES ($1, 'hl_ws_l2book', 'ETH', 'hl-book-1', 99.0, 5.0, 100.0, 3.0, 1.0)
            """,
            book_time,
        )
        await connection.execute(
            """
            INSERT INTO bn_depth (
                time, source, symbol, final_update_id, bids, asks,
                best_bid_price, best_ask_price
            )
            VALUES (
                $1,
                'bn_ws_depth',
                'BTCUSDT',
                31,
                '[["100","3"],["99","1"]]'::jsonb,
                '[["101","1"],["102","1"]]'::jsonb,
                100.0,
                101.0
            )
            """,
            book_time,
        )
        await connection.execute(
            """
            INSERT INTO hl_candles (
                time, source, coin, interval, open_time, close_time,
                open, high, low, close, volume, is_closed, event_hash
            )
            VALUES
                ($1, 'hl_ws_candles', 'ETH', '1m', $1, $2, 10.0, 14.0, 9.5, 13.0, 100.0, FALSE, 'hl-candle-open'),
                ($2, 'hl_ws_candles', 'ETH', '1m', $1, $2, 10.0, 14.0, 9.5, 13.0, 100.0, TRUE, 'hl-candle-closed')
            """,
            candle_open_time,
            candle_close_time,
        )
        await connection.execute(
            """
            INSERT INTO bn_klines (
                time, source, symbol, interval, pair, contract_type, open_time,
                close_time, open, high, low, close, volume, is_closed, event_hash
            )
            VALUES
                ($2, 'bn_ws_candles', 'BTCUSDT', '1m', '', '', $1, $2, 20.0, 24.0, 19.0, 23.0, 210.0, TRUE, 'bn-candle-ws'),
                ($1, 'bn_rest_klines', 'BTCUSDT', '1m', '', '', $1, $2, 99.0, 99.0, 99.0, 99.0, 999.0, FALSE, NULL),
                ($1, 'bn_rest_mark_kl', 'BTCUSDT', '1m', '', '', $1, $2, 77.0, 77.0, 77.0, 77.0, 777.0, TRUE, NULL)
            """,
            candle_open_time,
            candle_close_time,
        )

        orderbook = await connection.fetch(
            """
            SELECT time, source, exchange, coin, spread, imbalance
            FROM v_orderbook
            ORDER BY exchange, coin
            """,
        )
        candles = await connection.fetch(
            """
            SELECT time, source, exchange, coin, interval, open, high, low, close, volume
            FROM v_candles
            ORDER BY exchange, coin
            """,
        )

    assert [dict(row) for row in orderbook] == [
        {
            "time": book_time,
            "source": "bn_ws_depth",
            "exchange": "binance",
            "coin": "BTC",
            "spread": 1.0,
            "imbalance": pytest.approx(0.5),
        },
        {
            "time": book_time,
            "source": "hl_ws_l2book",
            "exchange": "hyperliquid",
            "coin": "ETH",
            "spread": 1.0,
            "imbalance": pytest.approx(0.25),
        },
    ]
    assert [dict(row) for row in candles] == [
        {
            "time": candle_open_time,
            "source": "bn_ws_candles",
            "exchange": "binance",
            "coin": "BTC",
            "interval": "1m",
            "open": 20.0,
            "high": 24.0,
            "low": 19.0,
            "close": 23.0,
            "volume": 210.0,
        },
        {
            "time": candle_open_time,
            "source": "hl_ws_candles",
            "exchange": "hyperliquid",
            "coin": "ETH",
            "interval": "1m",
            "open": 10.0,
            "high": 14.0,
            "low": 9.5,
            "close": 13.0,
            "volume": 100.0,
        },
    ]


@pytest.mark.asyncio
async def test_v_candles_excludes_open_binance_rest_klines(
    migrated_db: dict[str, object],
) -> None:
    candle_open_time = _utc("2026-03-22T10:30:00+06:00")
    candle_close_time = _utc("2026-03-22T10:31:00+06:00")
    pool = migrated_db["pool"]

    async with pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO bn_klines (
                time, source, symbol, interval, pair, contract_type, open_time,
                close_time, open, high, low, close, volume, is_closed, event_hash
            )
            VALUES (
                $1,
                'bn_rest_klines',
                'BTCUSDT',
                '1m',
                '',
                '',
                $1,
                $2,
                50.0,
                55.0,
                49.0,
                54.0,
                150.0,
                FALSE,
                NULL
            )
            """,
            candle_open_time,
            candle_close_time,
        )
        count = await connection.fetchval(
            "SELECT count(*) FROM v_candles WHERE source = 'bn_rest_klines'",
        )

    assert count == 0
