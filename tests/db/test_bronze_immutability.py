from __future__ import annotations

import asyncpg
import pytest

from ._bronze_expectations import EXPECTED_BRONZE_TABLES
from ._bronze_immutability_helpers import (
    HL_REST_FUNDING_MUTATIONS,
    HL_REST_FUNDING_TRIGGER_NAMES,
    immutability_trigger_pairs,
    trigger_pairs,
    utc,
)


@pytest.mark.asyncio
async def test_bronze_tables_reject_updates_and_deletes(
    migrated_db: dict[str, object],
) -> None:
    non_bronze_table = "test_mutable_metrics"
    pool = migrated_db["pool"]
    applied = set(migrated_db["applied"])
    assert {
        "0025_bronze_immutability_guards.sql",
        "0026_bronze_immutability_registry.sql",
    }.issubset(applied)

    async with pool.acquire() as connection:
        registry_rows = await connection.fetch(
            """
            SELECT table_name
            FROM bronze_table_registry
            WHERE table_schema = 'public'
            """,
        )
        helper_rows = await connection.fetch(
            "SELECT unnest(bronze_table_names()) AS table_name",
        )
        hypertable_rows = await connection.fetch(
            """
            SELECT hypertable_name
            FROM timescaledb_information.hypertables
            WHERE hypertable_schema = 'public'
              AND hypertable_name = ANY($1::text[])
            """,
            list(EXPECTED_BRONZE_TABLES),
        )
        expected_pairs = immutability_trigger_pairs(EXPECTED_BRONZE_TABLES)
        expected_trigger_names = {trigger_name for _, trigger_name in expected_pairs}
        trigger_rows = await connection.fetch(
            """
            SELECT c.relname AS table_name, t.tgname
            FROM pg_trigger AS t
            JOIN pg_class AS c ON c.oid = t.tgrelid
            JOIN pg_namespace AS n ON n.oid = c.relnamespace
            WHERE NOT t.tgisinternal
              AND n.nspname = 'public'
              AND c.relname = ANY($1::text[])
            """,
            list(EXPECTED_BRONZE_TABLES),
        )
        assert {row["table_name"] for row in registry_rows} == EXPECTED_BRONZE_TABLES
        assert {row["table_name"] for row in helper_rows} == EXPECTED_BRONZE_TABLES
        assert {
            row["hypertable_name"] for row in hypertable_rows
        } == EXPECTED_BRONZE_TABLES
        assert expected_pairs == trigger_pairs(trigger_rows, expected_trigger_names)

        event_time = utc("2026-03-22T00:05:00+00:00")
        await connection.execute(
            f"""
            CREATE TABLE {non_bronze_table} (
                time TIMESTAMPTZ NOT NULL,
                source TEXT NOT NULL,
                value DOUBLE PRECISION NOT NULL
            )
            """,
        )
        await connection.execute(
            "SELECT create_hypertable($1, 'time', if_not_exists => TRUE)",
            non_bronze_table,
        )
        non_bronze_trigger_rows = await connection.fetch(
            """
            SELECT t.tgname
            FROM pg_trigger AS t
            JOIN pg_class AS c ON c.oid = t.tgrelid
            JOIN pg_namespace AS n ON n.oid = c.relnamespace
            WHERE NOT t.tgisinternal
              AND n.nspname = 'public'
              AND c.relname = $1
            """,
            non_bronze_table,
        )
        assert not {
            row["tgname"]
            for row in non_bronze_trigger_rows
            if "reject" in row["tgname"]
        }

        await connection.execute(
            """
            INSERT INTO hl_rest_funding (time, source, coin, funding)
            VALUES ($1, 'hl_rest_funding', 'ETH', 0.01)
            """,
            event_time,
        )
        await connection.execute(
            f"""
            INSERT INTO {non_bronze_table} (time, source, value)
            VALUES ($1, 'manual', 1.0)
            """,
            event_time,
        )

        for statement in HL_REST_FUNDING_MUTATIONS:
            with pytest.raises(asyncpg.RaiseError, match="immutable"):
                await connection.execute(statement)

        await connection.execute(
            f"""
            UPDATE {non_bronze_table}
            SET value = 2.0
            WHERE source = 'manual'
            """,
        )
        await connection.execute(
            f"""
            DELETE FROM {non_bronze_table}
            WHERE source = 'manual'
            """,
        )
        await connection.execute(
            f"""
            INSERT INTO {non_bronze_table} (time, source, value)
            VALUES ($1, 'manual', 3.0)
            """,
            event_time,
        )
        await connection.execute(f"TRUNCATE {non_bronze_table}")

        await connection.execute(
            "DROP TRIGGER hl_rest_funding_reject_mutation_before_change ON hl_rest_funding"
        )
        await connection.execute(
            "DROP TRIGGER hl_rest_funding_reject_truncate_before_truncate ON hl_rest_funding"
        )
        await connection.execute(
            "SELECT refresh_bronze_immutability_guards('public', ARRAY['hl_rest_funding'])"
        )
        refreshed_rows = await connection.fetch(
            """
            SELECT c.relname AS table_name, t.tgname
            FROM pg_trigger AS t
            JOIN pg_class AS c ON c.oid = t.tgrelid
            JOIN pg_namespace AS n ON n.oid = c.relnamespace
            WHERE NOT t.tgisinternal
              AND n.nspname = 'public'
              AND c.relname = 'hl_rest_funding'
            """,
        )
        row = await connection.fetchrow(
            """
            SELECT source, coin, funding, time
            FROM hl_rest_funding
            WHERE source = 'hl_rest_funding'
              AND coin = 'ETH'
            """,
        )
        mutable_count = await connection.fetchval(
            f"SELECT count(*) FROM {non_bronze_table}",
        )

        assert trigger_pairs(
            refreshed_rows,
            HL_REST_FUNDING_TRIGGER_NAMES,
        ) == {
            ("hl_rest_funding", "hl_rest_funding_reject_mutation_before_change"),
            ("hl_rest_funding", "hl_rest_funding_reject_truncate_before_truncate"),
        }
        for statement in HL_REST_FUNDING_MUTATIONS:
            with pytest.raises(asyncpg.RaiseError, match="immutable"):
                await connection.execute(statement)

    assert row is not None
    assert (
        row["source"],
        row["coin"],
        row["funding"],
        row["time"],
        mutable_count,
    ) == ("hl_rest_funding", "ETH", 0.01, event_time, 0)
