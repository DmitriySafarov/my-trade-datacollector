from __future__ import annotations

from datetime import datetime, timezone

import asyncpg
import pytest


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)


@pytest.mark.asyncio
async def test_binance_rest_delivery_uses_exact_replay_identity(
    migrated_db: dict[str, object],
) -> None:
    poll_time = _utc("2026-03-22T06:10:00+06:00")
    later_poll_time = _utc("2026-03-22T06:11:00+06:00")
    first_delivery = _utc("2026-03-28T08:00:00+00:00")
    pool = migrated_db["pool"]

    async with pool.acquire() as connection:
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                """
                INSERT INTO bn_rest_delivery (time, source, pair)
                VALUES ($1, 'bn_rest_delivery', 'BTCUSD')
                """,
                poll_time,
            )

        for delivery_time in (
            first_delivery,
            _utc("2026-06-26T08:00:00+00:00"),
        ):
            await connection.execute(
                """
                INSERT INTO bn_rest_delivery (
                    time,
                    source,
                    pair,
                    delivery_time,
                    delivery_price
                )
                VALUES ($1, 'bn_rest_delivery', 'BTCUSD', $2, 43000.0)
                ON CONFLICT DO NOTHING
                """,
                poll_time,
                delivery_time,
            )
        await connection.execute(
            """
            INSERT INTO bn_rest_delivery (
                time,
                source,
                pair,
                delivery_time,
                delivery_price
            )
            VALUES ($1, 'bn_rest_delivery', 'BTCUSD', $2, 43000.0)
            ON CONFLICT DO NOTHING
            """,
            poll_time,
            first_delivery,
        )
        await connection.execute(
            """
            INSERT INTO bn_rest_delivery (
                time,
                source,
                pair,
                delivery_time,
                delivery_price
            )
            VALUES ($1, 'bn_rest_delivery', 'BTCUSD', $2, 43100.0)
            ON CONFLICT DO NOTHING
            """,
            later_poll_time,
            first_delivery,
        )

        row_count = await connection.fetchval(
            """
            SELECT count(*)
            FROM bn_rest_delivery
            WHERE source = 'bn_rest_delivery'
              AND pair = 'BTCUSD'
              AND time = $1
            """,
            poll_time,
        )
        delivery_history_rows = await connection.fetchval(
            """
            SELECT count(*)
            FROM bn_rest_delivery
            WHERE source = 'bn_rest_delivery'
              AND pair = 'BTCUSD'
              AND delivery_time = $1
            """,
            first_delivery,
        )

    assert row_count == 2
    assert delivery_history_rows == 2
