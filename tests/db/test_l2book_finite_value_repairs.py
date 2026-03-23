from __future__ import annotations

from pathlib import Path

import asyncpg
import pytest

from src.db.migrations import run_migrations

from ._migration_upgrade import upgrade_db, utc, write_migration_subset


def _pre_finite_guard_filenames() -> set[str]:
    root = Path(__file__).resolve().parents[2] / "migrations"
    return {
        path.name
        for path in root.glob("*.sql")
        if "0012_bronze_dedup_repairs.sql" <= path.name
        and path.name < "0035_hl_l2book_finite_value_guards.sql"
    }


@pytest.mark.asyncio
async def test_l2book_finite_guard_repair_filters_legacy_overflow_rows(
    tmp_path: Path,
) -> None:
    async with upgrade_db(tmp_path) as pool:
        await run_migrations(
            pool,
            write_migration_subset(
                tmp_path,
                "repair_l2book_finite_pre",
                _pre_finite_guard_filenames(),
            ),
        )

        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO hl_l2book (
                    time, source, coin, snapshot_hash, best_bid_price, best_bid_size,
                    best_ask_price, best_ask_size, spread, bid_notional, ask_notional
                )
                VALUES
                    (
                        $1, 'hl_ws_l2book', 'ETH', 'overflow-snap',
                        'Infinity'::float8, 1.0, 101.0, 1.0, '-Infinity'::float8,
                        'Infinity'::float8, 101.0
                    ),
                    (
                        $2, 'hl_ws_l2book', 'ETH', 'valid-snap',
                        200.0, 2.0, 201.0, 2.0, 1.0, 400.0, 402.0
                    )
                """,
                utc("2026-03-22T03:00:00+00:00"),
                utc("2026-03-22T03:00:01+00:00"),
            )

        await run_migrations(
            pool,
            write_migration_subset(
                tmp_path,
                "repair_l2book_finite_only",
                {"0035_hl_l2book_finite_value_guards.sql"},
            ),
        )

        async with pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT source, coin, best_bid, best_ask
                FROM v_orderbook
                WHERE exchange = 'hyperliquid'
                ORDER BY best_bid
                """
            )

            with pytest.raises(asyncpg.CheckViolationError):
                await connection.execute(
                    """
                    INSERT INTO hl_l2book (
                        time, source, coin, snapshot_hash, best_bid_price
                    )
                    VALUES ($1, 'hl_ws_l2book', 'ETH', 'future-overflow', 'Infinity'::float8)
                    """,
                    utc("2026-03-22T03:01:00+00:00"),
                )
            with pytest.raises(asyncpg.CheckViolationError):
                await connection.execute(
                    """
                    INSERT INTO hl_l2book (
                        time, source, coin, snapshot_hash, best_bid_price
                    )
                    VALUES ($1, 'hl_ws_l2book', 'ETH', 'zero-price', 0.0)
                    """,
                    utc("2026-03-22T03:01:01+00:00"),
                )
            registry_rows = await connection.fetchval(
                """
                SELECT count(*)
                FROM bronze_replay_registry
                WHERE table_name = 'hl_l2book'
                  AND source = 'hl_ws_l2book'
                  AND dedup_key IN ('future-overflow', 'zero-price')
                """
            )

    assert [dict(row) for row in rows] == [
        {
            "source": "hl_ws_l2book",
            "coin": "ETH",
            "best_bid": 200.0,
            "best_ask": 201.0,
        }
    ]
    assert registry_rows == 0
