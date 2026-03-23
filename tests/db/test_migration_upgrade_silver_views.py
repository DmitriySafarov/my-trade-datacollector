from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from src.db.migrations import run_migrations
from src.db.pool import close_pool
from tests._db_connection import create_pool_with_selected_config
from tests._db_test_support import _ensure_postgres_service, _test_configs
from tests.db._migration_upgrade import ROOT, write_migration_subset


@pytest.mark.asyncio
async def test_silver_migrations_apply_on_top_of_0026_schema(
    tmp_path: Path,
) -> None:
    await _ensure_postgres_service()
    admin_pool = None
    pool = None
    cleanup_may_raise = True
    cleanup_error: Exception | None = None
    db_name = f"collector_upgrade_{uuid.uuid4().hex[:12]}"

    try:
        admin_pool, _ = await create_pool_with_selected_config(
            _test_configs("postgres")
        )
        async with admin_pool.acquire() as connection:
            await connection.execute(f'CREATE DATABASE "{db_name}"')

        pool, _ = await create_pool_with_selected_config(
            _test_configs(db_name),
            allow_missing_database=True,
        )
        base_dir = write_migration_subset(
            tmp_path,
            "base_migrations",
            {
                path.name
                for path in (ROOT / "migrations").glob("*.sql")
                if path.name <= "0026_bronze_immutability_registry.sql"
            },
        )

        await run_migrations(pool, base_dir)
        applied = await run_migrations(pool, ROOT / "migrations")

        async with pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT table_name
                FROM information_schema.views
                WHERE table_schema = 'public'
                  AND table_name = ANY($1::text[])
                ORDER BY table_name
                """,
                [
                    "v_candles",
                    "v_funding",
                    "v_news_deduped",
                    "v_oi",
                    "v_orderbook",
                    "v_trades",
                ],
            )
        cleanup_may_raise = False
    finally:
        if pool is not None:
            try:
                await close_pool(pool)
            except Exception as error:
                cleanup_error = cleanup_error or error
        if admin_pool is not None:
            try:
                async with admin_pool.acquire() as connection:
                    await connection.execute(
                        f'DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)'
                    )
            except Exception as error:
                cleanup_error = cleanup_error or error
            try:
                await close_pool(admin_pool)
            except Exception as error:
                cleanup_error = cleanup_error or error
        if cleanup_error is not None and not cleanup_may_raise:
            raise cleanup_error

    required_versions = [
        "0027_silver_views.sql",
        "0028_silver_views.sql",
        "0029_silver_news_view.sql",
        "0030_silver_market_replay_repairs.sql",
        "0031_silver_news_identity_repairs.sql",
    ]
    assert [
        version for version in applied if version in required_versions
    ] == required_versions
    assert [row["table_name"] for row in rows] == [
        "v_candles",
        "v_funding",
        "v_news_deduped",
        "v_oi",
        "v_orderbook",
        "v_trades",
    ]


def test_write_migration_subset_raises_for_missing_files(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="9999_missing.sql"):
        write_migration_subset(tmp_path, "missing_subset", {"9999_missing.sql"})
