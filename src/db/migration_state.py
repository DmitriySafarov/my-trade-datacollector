from __future__ import annotations

from dataclasses import dataclass

import asyncpg


MIGRATION_METADATA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS schema_migrations (
        version TEXT PRIMARY KEY,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    ALTER TABLE schema_migrations
        ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'applied'
    """,
    """
    ALTER TABLE schema_migrations
        ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ
    """,
    """
    ALTER TABLE schema_migrations
        ADD COLUMN IF NOT EXISTS failed_at TIMESTAMPTZ
    """,
    """
    ALTER TABLE schema_migrations
        ADD COLUMN IF NOT EXISTS content_hash TEXT
    """,
)


class IncompleteNonTransactionalMigrationError(RuntimeError):
    pass


@dataclass(frozen=True)
class MigrationRecord:
    status: str
    content_hash: str | None


async def ensure_migration_metadata(connection: asyncpg.Connection) -> None:
    for statement in MIGRATION_METADATA_STATEMENTS:
        await connection.execute(statement, timeout=None)


async def load_migration_records(
    connection: asyncpg.Connection,
) -> dict[str, MigrationRecord]:
    rows = await connection.fetch(
        "SELECT version, status, content_hash FROM schema_migrations",
        timeout=None,
    )
    return {
        row["version"]: MigrationRecord(
            status=row["status"],
            content_hash=row["content_hash"],
        )
        for row in rows
    }


async def mark_migration_running(
    connection: asyncpg.Connection,
    version: str,
    content_hash: str,
) -> None:
    await connection.execute(
        """
        INSERT INTO schema_migrations (
            version, status, started_at, failed_at, content_hash
        )
        VALUES ($1, 'running', now(), NULL, $2)
        ON CONFLICT (version) DO UPDATE
        SET status = 'running',
            started_at = now(),
            failed_at = NULL,
            content_hash = EXCLUDED.content_hash
        """,
        version,
        content_hash,
        timeout=None,
    )


async def mark_migration_applied(
    connection: asyncpg.Connection,
    version: str,
    content_hash: str,
) -> None:
    await connection.execute(
        """
        INSERT INTO schema_migrations (
            version, status, started_at, applied_at, failed_at, content_hash
        )
        VALUES ($1, 'applied', now(), now(), NULL, $2)
        ON CONFLICT (version) DO UPDATE
        SET status = 'applied',
            started_at = COALESCE(schema_migrations.started_at, now()),
            applied_at = now(),
            failed_at = NULL,
            content_hash = EXCLUDED.content_hash
        """,
        version,
        content_hash,
        timeout=None,
    )


async def mark_migration_failed(
    connection: asyncpg.Connection,
    version: str,
) -> None:
    await connection.execute(
        """
        UPDATE schema_migrations
        SET status = 'failed',
            failed_at = now()
        WHERE version = $1
        """,
        version,
        timeout=None,
    )


async def backfill_migration_hash(
    connection: asyncpg.Connection,
    version: str,
    content_hash: str,
) -> None:
    await connection.execute(
        """
        UPDATE schema_migrations
        SET content_hash = $2
        WHERE version = $1
          AND status = 'applied'
          AND content_hash IS NULL
        """,
        version,
        content_hash,
        timeout=None,
    )
