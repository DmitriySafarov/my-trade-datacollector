from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from src.collectors.hyperliquid.l2book_parsing import parse_hyperliquid_l2book
from src.collectors.hyperliquid.l2book_storage import HyperliquidL2BookStore
from src.db.migrations import run_migrations
from src.db.migration_runtime import connect_for_migrations
from src.db.migration_sql import split_sql_statements
from tests._db_test_support import ROOT

from ._migration_upgrade import compress_tables, upgrade_db, utc, write_migration_subset


def _reconciliation_statements() -> list[str]:
    return split_sql_statements(
        (ROOT / "migrations" / "0033_ws_replay_registry_reconciliation.sql").read_text(
            encoding="utf-8"
        )
    )


def _pre_reconcile_filenames() -> set[str]:
    return {
        path.name
        for path in (ROOT / "migrations").glob("*.sql")
        if "0012_bronze_dedup_repairs.sql" <= path.name
        and path.name < "0033_ws_replay_registry_reconciliation.sql"
    }


def _same_key_row(time_ms: int) -> tuple[object, ...]:
    record = parse_hyperliquid_l2book(
        {
            "coin": "ETH",
            "time": time_ms,
            "levels": [
                [{"px": "3021.5", "sz": "8.2", "n": 3}],
                [{"px": "3022.0", "sz": "6.1", "n": 2}],
            ],
        },
        source="hl_ws_l2book",
        allowed_coins=("ETH",),
    ).as_copy_row()
    row = list(record)
    row[5] = "gap-snap"
    return tuple(row)


@pytest.mark.asyncio
async def test_reconciliation_same_key_copy_waits_for_commit_and_deduplicates(
    tmp_path: Path,
) -> None:
    async with upgrade_db(tmp_path) as pool:
        pre_reconcile = write_migration_subset(
            tmp_path,
            "repair_ws_same_key_pre",
            _pre_reconcile_filenames(),
        )
        await run_migrations(pool, pre_reconcile)

        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO hl_l2book (time, source, coin, snapshot_hash)
                VALUES ($1, 'hl_ws_l2book', 'ETH', 'gap-snap')
                """,
                utc("2026-03-22T02:20:00+00:00"),
            )
            await connection.execute(
                """
                DELETE FROM bronze_replay_registry
                WHERE table_name = 'hl_l2book'
                  AND source = 'hl_ws_l2book'
                  AND scope_1 = 'ETH'
                  AND dedup_key = 'gap-snap'
                """
            )

        repair_connection = await connect_for_migrations(pool)
        transaction = repair_connection.transaction()
        store_task: asyncio.Task[None] | None = None
        started = False
        committed = False
        try:
            await transaction.start()
            started = True
            for statement in _reconciliation_statements():
                await repair_connection.execute(statement)

            store_task = asyncio.create_task(
                HyperliquidL2BookStore(pool).write_many([_same_key_row(1710000000555)])
            )
            await asyncio.sleep(0.1)
            assert store_task.done() is False

            await transaction.commit()
            committed = True
            await asyncio.wait_for(store_task, timeout=1.0)
        finally:
            if store_task is not None and not store_task.done():
                store_task.cancel()
                await asyncio.gather(store_task, return_exceptions=True)
            if started and not committed:
                await transaction.rollback()
            await repair_connection.close()

        async with pool.acquire() as connection:
            counts = await connection.fetchrow(
                """
                SELECT
                    (SELECT count(*) FROM hl_l2book WHERE snapshot_hash = 'gap-snap') AS row_count,
                    (SELECT count(*) FROM bronze_replay_registry
                     WHERE table_name = 'hl_l2book'
                       AND source = 'hl_ws_l2book'
                       AND scope_1 = 'ETH'
                       AND dedup_key = 'gap-snap') AS registry_count
                """
            )

    assert dict(counts) == {"row_count": 1, "registry_count": 1}


@pytest.mark.asyncio
async def test_reconciliation_repairs_missing_registry_on_compressed_chunks(
    tmp_path: Path,
) -> None:
    async with upgrade_db(tmp_path) as pool:
        pre_reconcile = write_migration_subset(
            tmp_path,
            "repair_ws_compressed_pre",
            _pre_reconcile_filenames(),
        )
        await run_migrations(pool, pre_reconcile)

        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO hl_l2book (time, source, coin, snapshot_hash)
                VALUES ($1, 'hl_ws_l2book', 'ETH', 'compressed-gap')
                """,
                utc("2026-03-22T02:30:00+00:00"),
            )
            await connection.execute(
                """
                DELETE FROM bronze_replay_registry
                WHERE table_name = 'hl_l2book'
                  AND source = 'hl_ws_l2book'
                  AND scope_1 = 'ETH'
                  AND dedup_key = 'compressed-gap'
                """
            )

        await compress_tables(pool, "hl_l2book")
        reconcile_dir = write_migration_subset(
            tmp_path,
            "repair_ws_compressed_only",
            {"0033_ws_replay_registry_reconciliation.sql"},
        )
        await run_migrations(pool, reconcile_dir)

        async with pool.acquire() as connection:
            registry_count = await connection.fetchval(
                """
                SELECT count(*)
                FROM bronze_replay_registry
                WHERE table_name = 'hl_l2book'
                  AND source = 'hl_ws_l2book'
                  AND scope_1 = 'ETH'
                  AND dedup_key = 'compressed-gap'
                """
            )
            await connection.execute(
                """
                INSERT INTO hl_l2book (time, source, coin, snapshot_hash)
                VALUES ($1, 'hl_ws_l2book', 'ETH', 'compressed-gap')
                """,
                utc("2026-03-22T02:30:01+00:00"),
            )
            row_count = await connection.fetchval(
                """
                SELECT count(*)
                FROM hl_l2book
                WHERE source = 'hl_ws_l2book'
                  AND coin = 'ETH'
                  AND snapshot_hash = 'compressed-gap'
                """
            )

    assert registry_count == 1
    assert row_count == 1
