from __future__ import annotations

from datetime import datetime, timezone

import pytest


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)


@pytest.mark.asyncio
async def test_hl_trade_replays_with_shifted_timestamps_are_deduplicated(
    migrated_db: dict[str, object],
) -> None:
    pool = migrated_db["pool"]

    async with pool.acquire() as connection:
        for timestamp in (
            _utc("2026-03-22T00:03:00+00:00"),
            _utc("2026-03-22T00:03:00.001+00:00"),
        ):
            await connection.execute(
                """
                INSERT INTO hl_trades (
                    time, source, coin, side, price, size, hash, tid, users, payload
                )
                VALUES (
                    $1,
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
                """,
                timestamp,
            )

        row_count = await connection.fetchval(
            """
            SELECT count(*)
            FROM hl_trades
            WHERE source = 'hl_ws_trades'
              AND tid = 11
            """,
        )

    assert row_count == 1
