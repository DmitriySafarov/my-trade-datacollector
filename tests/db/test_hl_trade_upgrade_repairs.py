from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from ._migration_upgrade import apply_repair_migrations, upgrade_db


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)


@pytest.mark.asyncio
async def test_upgrade_removes_invalid_hl_trades_and_repairs_registry(
    tmp_path: Path,
) -> None:
    async with upgrade_db(tmp_path) as pool:
        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO hl_trades (
                    time, source, coin, side, price, size, hash, tid, users, payload
                )
                VALUES
                    (
                        '2026-03-22T00:00:00+00:00',
                        'hl_ws_trades',
                        'ETH',
                        'B',
                        10.0,
                        1.0,
                        'hash-valid',
                        11,
                        '["0xaaa","0xbbb"]'::jsonb,
                        '{"tid":11}'::jsonb
                    ),
                    (
                        '2026-03-22T00:00:01+00:00',
                        'hl_ws_trades',
                        'ETH',
                        'B',
                        0.0,
                        1.0,
                        'hash-invalid',
                        12,
                        '["0xaaa","0xbbb"]'::jsonb,
                        '{"tid":12}'::jsonb
                    )
                """
            )

        await apply_repair_migrations(pool, tmp_path, "repair_hl_trade_values")

        async with pool.acquire() as connection:
            counts = await connection.fetchrow(
                """
                SELECT
                    (SELECT count(*) FROM hl_trades WHERE tid = 11) AS valid_rows,
                    (SELECT count(*) FROM hl_trades WHERE tid = 12) AS invalid_rows,
                    (SELECT count(*) FROM hl_trade_registry WHERE source = 'hl_ws_trades' AND coin = 'ETH' AND tid = 11) AS valid_registry_rows,
                    (SELECT count(*) FROM hl_trade_registry WHERE source = 'hl_ws_trades' AND coin = 'ETH' AND tid = 12) AS invalid_registry_rows,
                    (SELECT count(*) FROM v_trades WHERE source = 'hl_ws_trades' AND coin = 'ETH') AS silver_rows
                """
            )

    assert dict(counts) == {
        "valid_rows": 1,
        "invalid_rows": 1,
        "valid_registry_rows": 1,
        "invalid_registry_rows": 0,
        "silver_rows": 1,
    }


@pytest.mark.asyncio
async def test_upgrade_excludes_null_hl_trade_values_from_registry_and_silver(
    tmp_path: Path,
) -> None:
    async with upgrade_db(tmp_path) as pool:
        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO hl_trades (
                    time, source, coin, side, price, size, hash, tid, users, payload
                )
                VALUES
                    ($1, 'hl_ws_trades', 'ETH', 'B', 10.0, 1.0, 'hash-valid', 21, '["0xaaa","0xbbb"]'::jsonb, '{"tid":21}'::jsonb),
                    ($2, 'hl_ws_trades', 'ETH', 'B', NULL, 1.0, 'hash-null-price', 22, '["0xaaa","0xbbb"]'::jsonb, '{"tid":22}'::jsonb),
                    ($3, 'hl_ws_trades', 'ETH', 'B', 10.0, NULL, 'hash-null-size', 23, '["0xaaa","0xbbb"]'::jsonb, '{"tid":23}'::jsonb)
                """,
                _utc("2026-03-22T00:01:00+00:00"),
                _utc("2026-03-22T00:01:01+00:00"),
                _utc("2026-03-22T00:01:02+00:00"),
            )

        await apply_repair_migrations(pool, tmp_path, "repair_hl_trade_values")

        async with pool.acquire() as connection:
            counts = await connection.fetchrow(
                """
                SELECT
                    (SELECT count(*) FROM hl_trade_registry WHERE source = 'hl_ws_trades' AND coin = 'ETH' AND tid = 21) AS valid_registry_rows,
                    (SELECT count(*) FROM hl_trade_registry WHERE source = 'hl_ws_trades' AND coin = 'ETH' AND tid = 22) AS null_price_registry_rows,
                    (SELECT count(*) FROM hl_trade_registry WHERE source = 'hl_ws_trades' AND coin = 'ETH' AND tid = 23) AS null_size_registry_rows,
                    (SELECT count(*) FROM v_trades WHERE source = 'hl_ws_trades' AND coin = 'ETH') AS silver_rows
                """
            )

    assert dict(counts) == {
        "valid_registry_rows": 1,
        "null_price_registry_rows": 0,
        "null_size_registry_rows": 0,
        "silver_rows": 1,
    }
