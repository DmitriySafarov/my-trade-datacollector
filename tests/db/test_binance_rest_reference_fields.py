from __future__ import annotations

from datetime import datetime, timezone

import pytest


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)


@pytest.mark.asyncio
async def test_binance_rest_metric_rows_keep_required_typed_fields(
    migrated_db: dict[str, object],
) -> None:
    event_time = _utc("2026-03-22T06:00:00+06:00")
    pool = migrated_db["pool"]

    async with pool.acquire() as connection:
        column_rows = await connection.fetch(
            """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND (
                (table_name = 'bn_rest_taker_vol' AND column_name = ANY($1::text[]))
                OR (table_name = 'bn_rest_basis' AND column_name = ANY($2::text[]))
              )
            """,
            ["buy_volume", "sell_volume"],
            ["index_price", "futures_price", "basis_rate"],
        )
        await connection.execute(
            """
            INSERT INTO bn_rest_taker_vol (
                time,
                source,
                symbol,
                contract_type,
                period,
                buy_volume,
                sell_volume,
                buy_sell_ratio
            )
            VALUES (
                $1,
                'bn_rest_taker_vol',
                'ETHUSDT',
                'PERPETUAL',
                '5m',
                120.0,
                80.0,
                1.5
            )
            ON CONFLICT DO NOTHING
            """,
            event_time,
        )
        await connection.execute(
            """
            INSERT INTO bn_rest_basis (
                time,
                source,
                pair,
                contract_type,
                period,
                index_price,
                futures_price,
                basis,
                basis_rate,
                annualized_basis_rate
            )
            VALUES (
                $1,
                'bn_rest_basis',
                'ETHUSDT',
                'PERPETUAL',
                '5m',
                3000.0,
                3006.0,
                6.0,
                0.002,
                0.73
            )
            ON CONFLICT DO NOTHING
            """,
            event_time,
        )
        taker_row = await connection.fetchrow(
            """
            SELECT source, time, buy_volume, sell_volume, buy_sell_ratio
            FROM bn_rest_taker_vol
            WHERE symbol = 'ETHUSDT'
            """,
        )
        basis_row = await connection.fetchrow(
            """
            SELECT
                source,
                time,
                index_price,
                futures_price,
                basis_rate,
                annualized_basis_rate
            FROM bn_rest_basis
            WHERE pair = 'ETHUSDT'
            """,
        )

    columns = {(row["table_name"], row["column_name"]) for row in column_rows}
    assert columns == {
        ("bn_rest_taker_vol", "buy_volume"),
        ("bn_rest_taker_vol", "sell_volume"),
        ("bn_rest_basis", "index_price"),
        ("bn_rest_basis", "futures_price"),
        ("bn_rest_basis", "basis_rate"),
    }
    assert taker_row["source"] == "bn_rest_taker_vol"
    assert taker_row["time"] == event_time
    assert taker_row["buy_volume"] == 120.0
    assert taker_row["sell_volume"] == 80.0
    assert taker_row["buy_sell_ratio"] == 1.5
    assert basis_row["source"] == "bn_rest_basis"
    assert basis_row["time"] == event_time
    assert basis_row["index_price"] == 3000.0
    assert basis_row["futures_price"] == 3006.0
    assert basis_row["basis_rate"] == 0.002
    assert basis_row["annualized_basis_rate"] == 0.73
