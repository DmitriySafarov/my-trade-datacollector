from __future__ import annotations

import asyncpg
import pytest


INVALID_HL_TRADES = [
    """
    INSERT INTO hl_trades (time, source, coin, side, price, size, hash, tid, users, payload)
    VALUES (
        '2026-03-22T00:00:00+00:00',
        'hl_ws_trades',
        'ETH',
        'B',
        10.0,
        1.0,
        NULL,
        11,
        '["0xaaa","0xbbb"]'::jsonb,
        '{"tid":11}'::jsonb
    )
    """,
    """
    INSERT INTO hl_trades (time, source, coin, side, price, size, hash, tid, users, payload)
    VALUES (
        '2026-03-22T00:00:01+00:00',
        'hl_ws_trades',
        'ETH',
        'X',
        10.0,
        1.0,
        'hash-12',
        12,
        '["0xaaa","0xbbb"]'::jsonb,
        '{"tid":12}'::jsonb
    )
    """,
    """
    INSERT INTO hl_trades (time, source, coin, side, price, size, hash, tid, users, payload)
    VALUES (
        '2026-03-22T00:00:02+00:00',
        'hl_ws_trades',
        'ETH',
        'B',
        'NaN'::float8,
        1.0,
        'hash-13',
        13,
        '["0xaaa","0xbbb"]'::jsonb,
        '{"tid":13}'::jsonb
    )
    """,
    """
    INSERT INTO hl_trades (time, source, coin, side, price, size, hash, tid, users, payload)
    VALUES (
        '1969-12-31T23:59:59+00:00',
        'hl_ws_trades',
        'ETH',
        'B',
        10.0,
        1.0,
        'hash-14',
        -14,
        '["0xaaa"]'::jsonb,
        '[]'::jsonb
    )
    """,
]


@pytest.mark.asyncio
@pytest.mark.parametrize("sql", INVALID_HL_TRADES)
async def test_hl_trades_reject_invalid_direct_inserts(
    migrated_db: dict[str, object],
    sql: str,
) -> None:
    pool = migrated_db["pool"]

    async with pool.acquire() as connection:
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(sql)
