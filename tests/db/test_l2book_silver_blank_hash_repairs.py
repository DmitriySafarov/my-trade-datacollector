from __future__ import annotations

from pathlib import Path

import pytest

from src.db.migrations import run_migrations

from ._migration_upgrade import upgrade_db, utc, write_migration_subset


def _pre_reconcile_filenames() -> set[str]:
    root = Path(__file__).resolve().parents[2] / "migrations"
    return {
        path.name
        for path in root.glob("*.sql")
        if "0012_bronze_dedup_repairs.sql" <= path.name
        and path.name < "0033_ws_replay_registry_reconciliation.sql"
    }


@pytest.mark.asyncio
async def test_reconciliation_excludes_blank_l2book_rows_from_v_orderbook(
    tmp_path: Path,
) -> None:
    async with upgrade_db(tmp_path) as pool:
        await run_migrations(
            pool,
            write_migration_subset(
                tmp_path,
                "repair_ws_blank_silver_pre",
                _pre_reconcile_filenames(),
            ),
        )

        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO hl_l2book (
                    time, source, coin, snapshot_hash, best_bid_price, best_bid_size,
                    best_ask_price, best_ask_size, spread, ingested_at
                )
                VALUES
                    ($1, 'hl_ws_l2book', 'ETH', '', 100.0, 1.0, 101.0, 1.0, 1.0, $2),
                    ($3, 'hl_ws_l2book', 'ETH', 'valid-snap', 200.0, 2.0, 201.0, 2.0, 1.0, $4)
                """,
                utc("2026-03-22T02:42:00+00:00"),
                utc("2026-03-22T02:42:10+00:00"),
                utc("2026-03-22T02:43:00+00:00"),
                utc("2026-03-22T02:43:10+00:00"),
            )

        await run_migrations(
            pool,
            write_migration_subset(
                tmp_path,
                "repair_ws_blank_silver_only",
                {"0033_ws_replay_registry_reconciliation.sql"},
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

    assert [dict(row) for row in rows] == [
        {
            "source": "hl_ws_l2book",
            "coin": "ETH",
            "best_bid": 200.0,
            "best_ask": 201.0,
        }
    ]
