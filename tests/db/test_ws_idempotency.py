from __future__ import annotations

from datetime import datetime, timezone

import asyncpg
import pytest


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)


@pytest.mark.asyncio
async def test_trade_replays_with_shifted_timestamps_are_deduplicated(
    migrated_db: dict[str, object],
) -> None:
    pool = migrated_db["pool"]

    async with pool.acquire() as connection:
        inserts = [
            (
                """
                INSERT INTO bn_agg_trades (time, source, symbol, agg_trade_id, price, qty)
                VALUES ($1, 'bn_ws_agg_trades', 'ETHUSDT', 21, 20.0, 2.0)
                """,
                "SELECT count(*) FROM bn_agg_trades WHERE source = 'bn_ws_agg_trades' AND agg_trade_id = 21",
                [
                    _utc("2026-03-22T00:06:00+00:00"),
                    _utc("2026-03-22T00:06:00.001+00:00"),
                ],
            ),
            (
                """
                INSERT INTO bn_agg_trades (time, source, symbol, agg_trade_id, price, qty)
                VALUES ($1, 'bn_rest_agg_trades', 'ETHUSDT', 22, 20.0, 2.0)
                """,
                "SELECT count(*) FROM bn_agg_trades WHERE source = 'bn_rest_agg_trades' AND agg_trade_id = 22",
                [
                    _utc("2026-03-22T00:07:00+00:00"),
                    _utc("2026-03-22T00:07:00.001+00:00"),
                ],
            ),
            (
                """
                INSERT INTO bn_agg_trades (time, source, symbol, trade_id, price, qty)
                VALUES ($1, 'bn_rest_trades', 'ETHUSDT', 23, 20.0, 2.0)
                """,
                "SELECT count(*) FROM bn_agg_trades WHERE source = 'bn_rest_trades' AND trade_id = 23",
                [
                    _utc("2026-03-22T00:08:00+00:00"),
                    _utc("2026-03-22T00:08:00.001+00:00"),
                ],
            ),
        ]

        for insert_sql, count_sql, timestamps in inserts:
            for timestamp in timestamps:
                await connection.execute(insert_sql, timestamp)
            assert await connection.fetchval(count_sql) == 1


@pytest.mark.asyncio
async def test_ws_replays_require_and_reuse_stable_dedup_keys(
    migrated_db: dict[str, object],
) -> None:
    pool = migrated_db["pool"]

    async with pool.acquire() as connection:
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                """
                INSERT INTO hl_l2book (time, source, coin)
                VALUES ('2026-03-22T00:10:00+00:00', 'hl_ws_l2book', 'ETH')
                """,
            )
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                """
                INSERT INTO bn_tickers (time, source, symbol, last_price)
                VALUES ('2026-03-22T00:10:01+00:00', 'bn_ws_book_ticker', 'ETHUSDT', 30.0)
                """,
            )

        replay_cases = [
            (
                """
                INSERT INTO hl_l2book (time, source, coin, snapshot_hash)
                VALUES ($1, 'hl_ws_l2book', 'ETH', 'snap-1')
                """,
                "SELECT count(*) FROM hl_l2book WHERE snapshot_hash = 'snap-1'",
            ),
            (
                """
                INSERT INTO hl_asset_ctx (time, source, coin, event_hash)
                VALUES ($1, 'hl_ws_asset_ctx', 'ETH', 'ctx-1')
                """,
                "SELECT count(*) FROM hl_asset_ctx WHERE event_hash = 'ctx-1'",
            ),
            (
                """
                INSERT INTO hl_candles (time, source, coin, interval, open_time, event_hash)
                VALUES ($1, 'hl_ws_candles', 'ETH', '1m', '2026-03-22T00:12:00+00:00', 'candle-1')
                """,
                "SELECT count(*) FROM hl_candles WHERE event_hash = 'candle-1'",
            ),
            (
                """
                INSERT INTO bn_depth (time, source, symbol, final_update_id)
                VALUES ($1, 'bn_ws_depth', 'ETHUSDT', 101)
                """,
                "SELECT count(*) FROM bn_depth WHERE final_update_id = 101",
            ),
            (
                """
                INSERT INTO bn_mark_price (time, source, symbol, event_hash)
                VALUES ($1, 'bn_ws_mark_price', 'ETHUSDT', 'mark-1')
                """,
                "SELECT count(*) FROM bn_mark_price WHERE event_hash = 'mark-1'",
            ),
            (
                """
                INSERT INTO bn_liquidations (time, source, symbol, event_hash)
                VALUES ($1, 'bn_ws_liquidations', 'ETHUSDT', 'liq-1')
                """,
                "SELECT count(*) FROM bn_liquidations WHERE event_hash = 'liq-1'",
            ),
            (
                """
                INSERT INTO bn_tickers (time, source, symbol, update_id, last_price)
                VALUES ($1, 'bn_ws_book_ticker', 'ETHUSDT', 301, 30.0)
                """,
                "SELECT count(*) FROM bn_tickers WHERE source = 'bn_ws_book_ticker' AND update_id = 301",
            ),
            (
                """
                INSERT INTO bn_tickers (time, source, symbol, event_hash, last_price)
                VALUES ($1, 'bn_ws_mini_ticker', 'ETHUSDT', 'mini-1', 30.0)
                """,
                "SELECT count(*) FROM bn_tickers WHERE source = 'bn_ws_mini_ticker' AND event_hash = 'mini-1'",
            ),
            (
                """
                INSERT INTO bn_klines (time, source, symbol, interval, open_time, event_hash)
                VALUES ($1, 'bn_ws_candles', 'ETHUSDT', '1m', '2026-03-22T00:18:00+00:00', 'bn-candle-1')
                """,
                "SELECT count(*) FROM bn_klines WHERE source = 'bn_ws_candles' AND event_hash = 'bn-candle-1'",
            ),
        ]

        for insert_sql, count_sql in replay_cases:
            for timestamp in (
                _utc("2026-03-22T00:20:00+00:00"),
                _utc("2026-03-22T00:20:00.001+00:00"),
            ):
                await connection.execute(insert_sql, timestamp)
            assert await connection.fetchval(count_sql) == 1


@pytest.mark.asyncio
async def test_rest_depth_polls_with_same_update_id_are_preserved(
    migrated_db: dict[str, object],
) -> None:
    pool = migrated_db["pool"]

    async with pool.acquire() as connection:
        for timestamp in (
            _utc("2026-03-22T00:30:00+00:00"),
            _utc("2026-03-22T00:31:00+00:00"),
        ):
            await connection.execute(
                """
                INSERT INTO bn_depth (time, source, symbol, final_update_id)
                VALUES ($1, 'bn_rest_depth', 'ETHUSDT', 777)
                """,
                timestamp,
            )

        row_count = await connection.fetchval(
            """
            SELECT count(*)
            FROM bn_depth
            WHERE source = 'bn_rest_depth'
              AND final_update_id = 777
            """,
        )

    assert row_count == 2
