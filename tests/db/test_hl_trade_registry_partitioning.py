from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_hl_trade_registry_is_partitioned_for_write_fanout(
    migrated_db: dict[str, object],
) -> None:
    pool = migrated_db["pool"]

    async with pool.acquire() as connection:
        row = await connection.fetchrow(
            """
            SELECT
                class.relkind,
                partitioned.partstrat,
                count(inherits.inhrelid) AS partition_count
            FROM pg_class AS class
            JOIN pg_namespace AS namespace ON namespace.oid = class.relnamespace
            LEFT JOIN pg_partitioned_table AS partitioned
                ON partitioned.partrelid = class.oid
            LEFT JOIN pg_inherits AS inherits ON inherits.inhparent = class.oid
            WHERE namespace.nspname = 'public'
              AND class.relname = 'hl_trade_registry'
            GROUP BY class.relkind, partitioned.partstrat
            """,
        )

    assert row["relkind"] in {"p", b"p"}
    assert row["partstrat"] in {"l", b"l"}
    assert row["partition_count"] == 32
