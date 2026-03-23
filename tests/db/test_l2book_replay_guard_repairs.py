from __future__ import annotations

from pathlib import Path

import asyncpg
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
async def test_reconciliation_skips_blank_l2book_keys_and_filters_silver(
    tmp_path: Path,
) -> None:
    async with upgrade_db(tmp_path) as pool:
        await run_migrations(
            pool,
            write_migration_subset(
                tmp_path,
                "repair_ws_blank_pre",
                _pre_reconcile_filenames(),
            ),
        )

        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO hl_l2book (
                    time, source, coin, snapshot_hash, best_bid_price, best_bid_size,
                    best_ask_price, best_ask_size, spread
                )
                VALUES
                    ($1, 'hl_ws_l2book', 'ETH', '', 100.0, 1.0, 101.0, 1.0, 1.0),
                    ($2, 'hl_ws_l2book', 'ETH', '   ', 150.0, 1.5, 151.0, 1.5, 1.0),
                    ($3, 'hl_ws_l2book', 'ETH', 'valid-snap', 200.0, 2.0, 201.0, 2.0, 1.0)
                """,
                utc("2026-03-22T02:40:00+00:00"),
                utc("2026-03-22T02:40:01+00:00"),
                utc("2026-03-22T02:40:02+00:00"),
            )
            await connection.execute(
                """
                INSERT INTO bronze_replay_registry (table_name, source, scope_1, scope_2, dedup_key)
                VALUES
                    ('hl_l2book', 'hl_ws_l2book', 'ETH', '', ''),
                    ('hl_l2book', 'hl_ws_l2book', 'ETH', '', '   ')
                ON CONFLICT DO NOTHING
                """
            )

        await run_migrations(
            pool,
            write_migration_subset(
                tmp_path,
                "repair_ws_blank_only",
                {"0033_ws_replay_registry_reconciliation.sql"},
            ),
        )

        async with pool.acquire() as connection:
            counts = await connection.fetchrow(
                """
                SELECT
                    (SELECT count(*)
                     FROM bronze_replay_registry
                     WHERE table_name = 'hl_l2book'
                       AND source = 'hl_ws_l2book'
                       AND btrim(dedup_key) = '') AS blank_registry_rows,
                    (SELECT count(*)
                     FROM hl_l2book
                     WHERE source = 'hl_ws_l2book'
                       AND btrim(snapshot_hash) = '') AS blank_bronze_rows,
                    (SELECT count(*)
                     FROM v_orderbook
                     WHERE source = 'hl_ws_l2book'
                       AND coin = 'ETH'
                       AND best_bid IN (100.0, 150.0)) AS blank_silver_rows,
                    (SELECT count(*)
                     FROM v_orderbook
                     WHERE source = 'hl_ws_l2book'
                       AND coin = 'ETH'
                       AND best_bid = 200.0) AS valid_silver_rows
                """
            )

            with pytest.raises(asyncpg.CheckViolationError):
                await connection.execute(
                    """
                    INSERT INTO hl_l2book (time, source, coin, snapshot_hash)
                    VALUES ($1, 'hl_ws_l2book', 'ETH', '')
                    """,
                    utc("2026-03-22T02:41:00+00:00"),
                )
            with pytest.raises(asyncpg.CheckViolationError):
                await connection.execute(
                    """
                    INSERT INTO hl_l2book (time, source, coin, snapshot_hash)
                    VALUES ($1, 'hl_ws_l2book', 'ETH', '   ')
                    """,
                    utc("2026-03-22T02:41:01+00:00"),
                )

    assert dict(counts) == {
        "blank_registry_rows": 0,
        "blank_bronze_rows": 2,
        "blank_silver_rows": 0,
        "valid_silver_rows": 1,
    }
