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

from ._migration_upgrade import (
    apply_repair_migrations,
    compress_tables,
    upgrade_db,
    utc,
    write_migration_subset,
)


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


def _l2book_record(*, time_ms: int):
    return parse_hyperliquid_l2book(
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
    )


@pytest.mark.asyncio
async def test_upgrade_repairs_ws_replay_registry_backfill(tmp_path: Path) -> None:
    async with upgrade_db(tmp_path) as pool:
        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO hl_l2book (time, source, coin, snapshot_hash, ingested_at)
                VALUES
                    ($1, 'hl_ws_l2book', 'ETH', 'snap-1', $2),
                    ($3, 'hl_ws_l2book', 'ETH', 'snap-1', $4)
                """,
                utc("2026-03-22T01:00:00+00:00"),
                utc("2026-03-22T01:10:00+00:00"),
                utc("2026-03-22T01:00:00.001+00:00"),
                utc("2026-03-22T01:11:00+00:00"),
            )
            await connection.execute(
                """
                INSERT INTO bn_depth (time, source, symbol, final_update_id, ingested_at)
                VALUES
                    ($1, 'bn_ws_depth', 'ETHUSDT', 101, $2),
                    ($3, 'bn_ws_depth', 'ETHUSDT', 101, $4),
                    ($5, 'bn_rest_depth', 'ETHUSDT', 777, $6),
                    ($7, 'bn_rest_depth', 'ETHUSDT', 777, $8)
                """,
                utc("2026-03-22T01:01:00+00:00"),
                utc("2026-03-22T01:20:00+00:00"),
                utc("2026-03-22T01:01:00.001+00:00"),
                utc("2026-03-22T01:21:00+00:00"),
                utc("2026-03-22T01:02:00+00:00"),
                utc("2026-03-22T01:22:00+00:00"),
                utc("2026-03-22T01:03:00+00:00"),
                utc("2026-03-22T01:23:00+00:00"),
            )
            await connection.execute(
                """
                INSERT INTO bn_tickers (time, source, symbol, update_id, event_hash, ingested_at)
                VALUES
                    ($1, 'bn_ws_book_ticker', 'ETHUSDT', 301, NULL, $2),
                    ($3, 'bn_ws_book_ticker', 'ETHUSDT', 301, NULL, $4),
                    ($5, 'bn_ws_ticker', 'ETHUSDT', NULL, 'ticker-1', $6),
                    ($7, 'bn_ws_ticker', 'ETHUSDT', NULL, 'ticker-1', $8)
                """,
                utc("2026-03-22T01:04:00+00:00"),
                utc("2026-03-22T01:24:00+00:00"),
                utc("2026-03-22T01:04:00.001+00:00"),
                utc("2026-03-22T01:25:00+00:00"),
                utc("2026-03-22T01:05:00+00:00"),
                utc("2026-03-22T01:26:00+00:00"),
                utc("2026-03-22T01:05:00.001+00:00"),
                utc("2026-03-22T01:27:00+00:00"),
            )

        await compress_tables(pool, "hl_l2book", "bn_depth", "bn_tickers")
        await apply_repair_migrations(pool, tmp_path, "repair_ws")

        async with pool.acquire() as connection:
            counts = await connection.fetchrow(
                """
                SELECT
                    (SELECT count(*) FROM hl_l2book WHERE snapshot_hash = 'snap-1') AS hl_l2book_rows,
                    (SELECT count(*) FROM bronze_replay_registry WHERE table_name = 'hl_l2book' AND source = 'hl_ws_l2book' AND dedup_key = 'snap-1') AS hl_l2book_registry_rows,
                    (SELECT count(*) FROM bn_depth WHERE source = 'bn_ws_depth' AND final_update_id = 101) AS bn_ws_depth_rows,
                    (SELECT count(*) FROM bronze_replay_registry WHERE table_name = 'bn_depth' AND source = 'bn_ws_depth' AND dedup_key = '101') AS bn_ws_depth_registry_rows,
                    (SELECT count(*) FROM bn_depth WHERE source = 'bn_rest_depth' AND final_update_id = 777) AS bn_rest_depth_rows,
                    (SELECT count(*) FROM bronze_replay_registry WHERE table_name = 'bn_depth' AND source = 'bn_rest_depth') AS rest_depth_registry_rows,
                    (SELECT count(*) FROM bn_tickers WHERE source = 'bn_ws_book_ticker' AND update_id = 301) AS book_ticker_rows,
                    (SELECT count(*) FROM bronze_replay_registry WHERE table_name = 'bn_tickers' AND source = 'bn_ws_book_ticker' AND dedup_key = '301') AS book_ticker_registry_rows,
                    (SELECT count(*) FROM bn_tickers WHERE source = 'bn_ws_ticker' AND event_hash = 'ticker-1') AS ticker_rows,
                    (SELECT count(*) FROM bronze_replay_registry WHERE table_name = 'bn_tickers' AND source = 'bn_ws_ticker' AND dedup_key = 'ticker-1') AS ticker_registry_rows
                """
            )

    assert dict(counts) == {
        "hl_l2book_rows": 2,
        "hl_l2book_registry_rows": 1,
        "bn_ws_depth_rows": 2,
        "bn_ws_depth_registry_rows": 1,
        "bn_rest_depth_rows": 2,
        "rest_depth_registry_rows": 0,
        "book_ticker_rows": 2,
        "book_ticker_registry_rows": 1,
        "ticker_rows": 2,
        "ticker_registry_rows": 1,
    }


@pytest.mark.asyncio
async def test_reconciliation_repairs_missing_l2book_registry_rows(
    tmp_path: Path,
) -> None:
    async with upgrade_db(tmp_path) as pool:
        pre_reconcile = write_migration_subset(
            tmp_path,
            "repair_ws_pre_reconcile",
            _pre_reconcile_filenames(),
        )
        await run_migrations(pool, pre_reconcile)

        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO hl_l2book (time, source, coin, snapshot_hash)
                VALUES ($1, 'hl_ws_l2book', 'ETH', 'gap-snap')
                """,
                utc("2026-03-22T02:00:00+00:00"),
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

        reconcile_dir = write_migration_subset(
            tmp_path,
            "repair_ws_reconcile_only",
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
                  AND dedup_key = 'gap-snap'
                """
            )
            await connection.execute(
                """
                INSERT INTO hl_l2book (time, source, coin, snapshot_hash)
                VALUES ($1, 'hl_ws_l2book', 'ETH', 'gap-snap')
                """,
                utc("2026-03-22T02:00:01+00:00"),
            )
            row_count = await connection.fetchval(
                """
                SELECT count(*)
                FROM hl_l2book
                WHERE source = 'hl_ws_l2book'
                  AND coin = 'ETH'
                  AND snapshot_hash = 'gap-snap'
                """
            )

    assert registry_count == 1
    assert row_count == 1


@pytest.mark.asyncio
async def test_reconciliation_allows_live_l2book_copy_writes_while_open(
    tmp_path: Path,
) -> None:
    async with upgrade_db(tmp_path) as pool:
        pre_reconcile = write_migration_subset(
            tmp_path,
            "repair_ws_pre_lock_check",
            _pre_reconcile_filenames(),
        )
        await run_migrations(pool, pre_reconcile)

        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO hl_l2book (time, source, coin, snapshot_hash)
                VALUES ($1, 'hl_ws_l2book', 'ETH', 'gap-snap')
                """,
                utc("2026-03-22T02:04:00+00:00"),
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
        started = False
        committed = False
        try:
            await transaction.start()
            started = True
            for statement in _reconciliation_statements():
                await repair_connection.execute(statement)

            live_record = _l2book_record(time_ms=1710000000456)
            await asyncio.wait_for(
                HyperliquidL2BookStore(pool).write_many([live_record.as_copy_row()]),
                timeout=1.0,
            )
            await transaction.commit()
            committed = True
        finally:
            if started and not committed:
                await transaction.rollback()
            await repair_connection.close()

        async with pool.acquire() as connection:
            counts = await connection.fetchrow(
                """
                SELECT
                    (SELECT count(*) FROM bronze_replay_registry
                     WHERE table_name = 'hl_l2book'
                       AND source = 'hl_ws_l2book'
                       AND scope_1 = 'ETH'
                       AND dedup_key = 'gap-snap') AS repaired_registry_count,
                    (SELECT count(*) FROM hl_l2book WHERE snapshot_hash = $1) AS row_count,
                    (SELECT count(*) FROM bronze_replay_registry
                     WHERE table_name = 'hl_l2book'
                       AND source = 'hl_ws_l2book'
                       AND scope_1 = 'ETH'
                       AND dedup_key = $1) AS registry_count
                """,
                live_record.snapshot_hash,
            )

    assert dict(counts) == {
        "repaired_registry_count": 1,
        "row_count": 1,
        "registry_count": 1,
    }


@pytest.mark.asyncio
async def test_reconciliation_leaves_other_registry_entries_unchanged(
    tmp_path: Path,
) -> None:
    async with upgrade_db(tmp_path) as pool:
        pre_reconcile = write_migration_subset(
            tmp_path,
            "repair_ws_other_registry",
            _pre_reconcile_filenames(),
        )
        await run_migrations(pool, pre_reconcile)

        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO bn_depth (time, source, symbol, final_update_id)
                VALUES ($1, 'bn_ws_depth', 'ETHUSDT', 401)
                """,
                utc("2026-03-22T02:10:00+00:00"),
            )
            before = await connection.fetchval(
                """
                SELECT count(*)
                FROM bronze_replay_registry
                WHERE table_name = 'bn_depth'
                  AND source = 'bn_ws_depth'
                  AND scope_1 = 'ETHUSDT'
                  AND dedup_key = '401'
                """
            )

        reconcile_dir = write_migration_subset(
            tmp_path,
            "repair_ws_other_registry_reconcile_only",
            {"0033_ws_replay_registry_reconciliation.sql"},
        )
        await run_migrations(pool, reconcile_dir)

        async with pool.acquire() as connection:
            after = await connection.fetchval(
                """
                SELECT count(*)
                FROM bronze_replay_registry
                WHERE table_name = 'bn_depth'
                  AND source = 'bn_ws_depth'
                  AND scope_1 = 'ETHUSDT'
                  AND dedup_key = '401'
                """
            )

    assert before == 1
    assert after == 1
