from __future__ import annotations

import asyncpg
import pytest


@pytest.mark.asyncio
async def test_source_checks_and_dedup_indexes_work(
    migrated_db: dict[str, object],
) -> None:
    pool = migrated_db["pool"]

    async with pool.acquire() as connection:
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                """
                INSERT INTO hl_asset_ctx (time, source, coin)
                VALUES ('2026-03-22T00:00:00+00:00', 'hl_rest_funding', 'ETH')
                """,
            )
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                """
                INSERT INTO bn_mark_price (time, source, symbol)
                VALUES ('2026-03-22T00:01:00+00:00', 'bn_rest_oi', 'ETHUSDT')
                """,
            )
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                """
                INSERT INTO bn_tickers (time, source, symbol)
                VALUES ('2026-03-22T00:02:00+00:00', 'bn_rest_exchange_info', 'ETHUSDT')
                """,
            )
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                """
                INSERT INTO hl_rest_candles (time, source, coin, interval, open_time)
                VALUES (
                    '2026-03-22T00:00:01+00:00',
                    'hl_rest_candles',
                    'ETH',
                    '1h',
                    '2026-03-22T00:00:00+00:00'
                )
                """,
            )
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                """
                INSERT INTO bn_klines (time, source, symbol, interval, open_time)
                VALUES (
                    '2026-03-22T00:00:01+00:00',
                    'bn_rest_klines',
                    'ETHUSDT',
                    '1m',
                    '2026-03-22T00:00:00+00:00'
                )
                """,
            )

        statements = [
            (
                """
                INSERT INTO hl_trades (
                    time, source, coin, side, price, size, hash, tid, users, payload
                )
                VALUES (
                    '2026-03-22T00:03:00+00:00',
                    'hl_ws_trades',
                    'ETH',
                    'B',
                    10.0,
                    1.0,
                    'hash-11',
                    11,
                    '["0xaaa","0xbbb"]'::jsonb,
                    '{"tid":11}'::jsonb
                )
                ON CONFLICT (source, time, coin, tid) WHERE tid IS NOT NULL DO NOTHING
                """,
                "SELECT count(*) FROM hl_trades WHERE tid = 11",
            ),
            (
                """
                INSERT INTO hl_l2book (time, source, coin, snapshot_hash)
                VALUES ('2026-03-22T00:04:00+00:00', 'hl_ws_l2book', 'ETH', 'snap-1')
                ON CONFLICT (source, time, coin, snapshot_hash)
                WHERE snapshot_hash IS NOT NULL DO NOTHING
                """,
                "SELECT count(*) FROM hl_l2book WHERE snapshot_hash = 'snap-1'",
            ),
            (
                """
                INSERT INTO hl_rest_funding (time, source, coin, funding)
                VALUES ('2026-03-22T00:05:00+00:00', 'hl_rest_funding', 'ETH', 0.01)
                ON CONFLICT (source, time, coin) DO NOTHING
                """,
                "SELECT count(*) FROM hl_rest_funding WHERE coin = 'ETH'",
            ),
            (
                """
                INSERT INTO hl_rest_candles (time, source, coin, interval, open_time)
                VALUES (
                    '2026-03-22T00:00:00+00:00',
                    'hl_rest_candles',
                    'ETH',
                    '1h',
                    '2026-03-22T00:00:00+00:00'
                )
                ON CONFLICT (source, time, coin, interval) DO NOTHING
                """,
                "SELECT count(*) FROM hl_rest_candles WHERE coin = 'ETH'",
            ),
            (
                """
                INSERT INTO bn_agg_trades (time, source, symbol, agg_trade_id, price, qty)
                VALUES ('2026-03-22T00:06:00+00:00', 'bn_ws_agg_trades', 'ETHUSDT', 21, 20.0, 2.0)
                ON CONFLICT (source, time, symbol, agg_trade_id)
                WHERE agg_trade_id IS NOT NULL DO NOTHING
                """,
                "SELECT count(*) FROM bn_agg_trades WHERE agg_trade_id = 21",
            ),
            (
                """
                INSERT INTO bn_tickers (time, source, symbol, update_id, last_price)
                VALUES ('2026-03-22T00:07:00+00:00', 'bn_ws_book_ticker', 'ETHUSDT', 31, 30.0)
                ON CONFLICT (source, time, symbol, update_id)
                WHERE update_id IS NOT NULL DO NOTHING
                """,
                "SELECT count(*) FROM bn_tickers WHERE update_id = 31",
            ),
            (
                """
                INSERT INTO bn_klines (
                    time, source, symbol, interval, pair, contract_type, open_time
                )
                VALUES (
                    '2026-03-22T00:08:00+00:00',
                    'bn_rest_klines',
                    'ETHUSDT',
                    '1m',
                    '',
                    '',
                    '2026-03-22T00:08:00+00:00'
                )
                ON CONFLICT (source, time, symbol, interval, contract_type, pair)
                WHERE source <> 'bn_ws_candles' DO NOTHING
                """,
                "SELECT count(*) FROM bn_klines WHERE source = 'bn_rest_klines'",
            ),
            (
                """
                INSERT INTO bn_rest_oi (time, source, symbol, open_interest)
                VALUES ('2026-03-22T00:09:00+00:00', 'bn_rest_oi', 'ETHUSDT', 42.0)
                ON CONFLICT (source, time, symbol) DO NOTHING
                """,
                "SELECT count(*) FROM bn_rest_oi WHERE symbol = 'ETHUSDT'",
            ),
            (
                """
                INSERT INTO bn_rest_exchange_info (time, source, symbol, status)
                VALUES ('2026-03-22T00:10:00+00:00', 'bn_rest_exchange_info', 'ETHUSDT', 'TRADING')
                ON CONFLICT (source, time, symbol) DO NOTHING
                """,
                "SELECT count(*) FROM bn_rest_exchange_info WHERE symbol = 'ETHUSDT'",
            ),
            (
                """
                INSERT INTO fred_data (time, source, series_id, value)
                VALUES ('2026-03-22T00:11:00+00:00', 'fred_daily', 'VIXCLS', 18.5)
                ON CONFLICT (source, time, series_id)
                WHERE series_id IS NOT NULL DO NOTHING
                """,
                "SELECT count(*) FROM fred_data WHERE series_id = 'VIXCLS'",
            ),
        ]

        for insert_sql, count_sql in statements:
            await connection.execute(insert_sql)
            await connection.execute(insert_sql)
            assert await connection.fetchval(count_sql) == 1
