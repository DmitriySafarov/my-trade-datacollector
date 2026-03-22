from __future__ import annotations

from datetime import timedelta

import pytest

from ._bronze_expectations import EXPECTED_BRONZE_TABLES, EXPECTED_NEW_MIGRATIONS


@pytest.mark.asyncio
async def test_bronze_tables_are_migrated_as_hypertables(
    migrated_db: dict[str, object],
) -> None:
    pool = migrated_db["pool"]
    applied = set(migrated_db["applied"])
    assert EXPECTED_NEW_MIGRATIONS.issubset(applied)

    async with pool.acquire() as connection:
        assert await connection.fetchval("SHOW TIMEZONE") in {"UTC", "Etc/UTC"}
        extension = await connection.fetchval(
            "SELECT extname FROM pg_extension WHERE extname = 'timescaledb'",
        )
        assert extension == "timescaledb"

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
        public_hypertable_rows = await connection.fetch(
            """
            SELECT hypertable_name
            FROM timescaledb_information.hypertables
            WHERE hypertable_schema = 'public'
            """,
        )
        hypertable_rows = await connection.fetch(
            """
            SELECT hypertable_name, compression_enabled
            FROM timescaledb_information.hypertables
            WHERE hypertable_schema = 'public'
              AND hypertable_name = ANY($1::text[])
            """,
            list(EXPECTED_BRONZE_TABLES),
        )
        column_rows = await connection.fetch(
            """
            SELECT table_name, column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = ANY($1::text[])
              AND column_name = ANY($2::text[])
            """,
            list(EXPECTED_BRONZE_TABLES),
            ["time", "source", "payload"],
        )
        policy_rows = await connection.fetch(
            """
            SELECT hypertable_name, (config->>'compress_after')::interval AS compress_after
            FROM timescaledb_information.jobs
            WHERE hypertable_schema = 'public'
              AND hypertable_name = ANY($1::text[])
              AND proc_name LIKE 'policy_compression%'
            """,
            list(EXPECTED_BRONZE_TABLES),
        )
        dimension_rows = await connection.fetch(
            """
            SELECT hypertable_name, time_interval
            FROM timescaledb_information.dimensions
            WHERE hypertable_schema = 'public'
              AND hypertable_name = ANY($1::text[])
              AND column_name = 'time'
            """,
            list(EXPECTED_BRONZE_TABLES),
        )

    expected_policies = {table: timedelta(hours=48) for table in EXPECTED_BRONZE_TABLES}
    expected_policies["s3_l2book"] = timedelta(days=45)
    registry_tables = {row["table_name"] for row in registry_rows}
    helper_tables = {row["table_name"] for row in helper_rows}
    hypertables = {row["hypertable_name"] for row in hypertable_rows}
    public_hypertables = {row["hypertable_name"] for row in public_hypertable_rows}
    compression = {
        row["hypertable_name"]: row["compression_enabled"] for row in hypertable_rows
    }
    columns = {
        (row["table_name"], row["column_name"]): (row["data_type"], row["is_nullable"])
        for row in column_rows
    }
    policies = {row["hypertable_name"]: row["compress_after"] for row in policy_rows}
    chunk_intervals = {
        row["hypertable_name"]: row["time_interval"] for row in dimension_rows
    }

    assert registry_tables == EXPECTED_BRONZE_TABLES
    assert helper_tables == EXPECTED_BRONZE_TABLES
    assert EXPECTED_BRONZE_TABLES.issubset(public_hypertables)
    assert EXPECTED_BRONZE_TABLES == hypertables
    assert EXPECTED_BRONZE_TABLES == set(policies)
    assert EXPECTED_BRONZE_TABLES == set(chunk_intervals)
    for table in EXPECTED_BRONZE_TABLES:
        assert compression[table] is True
        assert policies[table] == expected_policies[table]
        assert columns[(table, "time")] == ("timestamp with time zone", "NO")
        assert columns[(table, "source")] == ("text", "NO")
        assert columns[(table, "payload")] == ("jsonb", "NO")


@pytest.mark.asyncio
async def test_compression_settings_cover_idempotency_columns(
    migrated_db: dict[str, object],
) -> None:
    required = {
        "bn_liquidations": {"source", "symbol", "time", "event_hash"},
        "bn_tickers": {"source", "symbol", "time", "update_id", "event_hash"},
        "bn_klines": {
            "source",
            "symbol",
            "interval",
            "time",
            "contract_type",
            "pair",
            "event_hash",
        },
        "bn_rest_taker_vol": {"source", "symbol", "period", "contract_type", "time"},
        "bn_rest_basis": {"source", "pair", "period", "contract_type", "time"},
        "bn_rest_delivery": {"source", "pair", "delivery_time", "time"},
        "news": {"source", "time", "dedup_url"},
        "news_sentiment": {"source", "time", "dedup_url"},
        "fred_data": {"source", "series_id", "time", "release_id"},
    }
    pool = migrated_db["pool"]

    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT
                hypertable_name,
                attname,
                segmentby_column_index,
                orderby_column_index
            FROM timescaledb_information.compression_settings
            WHERE hypertable_schema = 'public'
              AND hypertable_name = ANY($1::text[])
            """,
            list(required),
        )

    covered: dict[str, set[str]] = {}
    for row in rows:
        if (
            row["segmentby_column_index"] is None
            and row["orderby_column_index"] is None
        ):
            continue
        covered.setdefault(row["hypertable_name"], set()).add(row["attname"])

    for table, columns in required.items():
        assert columns.issubset(covered.get(table, set()))


@pytest.mark.asyncio
async def test_replay_guards_are_installed(migrated_db: dict[str, object]) -> None:
    expected_triggers = {
        "hl_trades_register_tid_before_insert",
        "bn_agg_trades_register_trade_id_before_insert",
        "news_register_dedup_url_before_insert",
        "news_sentiment_register_dedup_url_before_insert",
        "bn_rest_delivery_register_delivery_time_before_insert",
        "hl_l2book_register_replay_key_before_insert",
        "hl_asset_ctx_register_replay_key_before_insert",
        "hl_candles_register_replay_key_before_insert",
        "bn_depth_register_replay_key_before_insert",
        "bn_mark_price_register_replay_key_before_insert",
        "bn_liquidations_register_replay_key_before_insert",
        "bn_tickers_register_replay_key_before_insert",
        "bn_klines_register_replay_key_before_insert",
    }
    pool = migrated_db["pool"]

    async with pool.acquire() as connection:
        trigger_rows = await connection.fetch(
            "SELECT tgname FROM pg_trigger WHERE NOT tgisinternal AND tgname = ANY($1::text[])",
            list(expected_triggers),
        )

    assert {row["tgname"] for row in trigger_rows} == expected_triggers
