from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_runtime_defaults_are_repaired_for_existing_databases(
    migrated_db: dict[str, object],
) -> None:
    expected = {
        ("schema_migrations", "applied_at"),
        ("settings", "updated_at"),
        ("source_watermarks", "updated_at"),
        ("gap_events", "created_at"),
        ("verification_reports", "created_at"),
    }
    pool = migrated_db["pool"]

    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT table_name, column_name, column_default
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND (table_name, column_name) IN (
                ('schema_migrations', 'applied_at'),
                ('settings', 'updated_at'),
                ('source_watermarks', 'updated_at'),
                ('gap_events', 'created_at'),
                ('verification_reports', 'created_at')
              )
            """,
        )

    defaults = {
        (row["table_name"], row["column_name"]): row["column_default"] for row in rows
    }
    assert set(defaults) == expected
    for default in defaults.values():
        assert "now()" in default
        assert "timezone(" not in default
