from __future__ import annotations

from pathlib import Path

import asyncpg


MIGRATIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now())
)
"""


async def run_migrations(pool: asyncpg.Pool, migrations_dir: Path) -> list[str]:
    files = sorted(migrations_dir.glob("*.sql"))
    applied: list[str] = []

    async with pool.acquire() as connection:
        await connection.execute(MIGRATIONS_TABLE_SQL)
        existing = await connection.fetch(
            "SELECT version FROM schema_migrations",
        )
        existing_versions = {row["version"] for row in existing}

        for migration_file in files:
            version = migration_file.name
            if version in existing_versions:
                continue

            sql = migration_file.read_text(encoding="utf-8")
            await connection.execute(sql)
            await connection.execute(
                """
                INSERT INTO schema_migrations (version)
                VALUES ($1)
                """,
                version,
            )
            applied.append(version)

    return applied
