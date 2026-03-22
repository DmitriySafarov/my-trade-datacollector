from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import asyncpg

from src.db.migration_files import parse_migration_sql
from src.db.migration_hash import migration_content_hash
from src.db.migration_manifest import (
    COMPATIBLE_MIGRATION_HASHES,
    STARTUP_NON_TRANSACTIONAL_MIGRATIONS,
    TRUSTED_MIGRATION_HASHES,
)
from src.db.migration_state import (
    IncompleteNonTransactionalMigrationError,
    backfill_migration_hash,
    load_migration_records,
)


class MigrationHashMismatchError(RuntimeError):
    pass


class MissingMigrationHashError(RuntimeError):
    pass


class NonTransactionalMigrationMaintenanceRequiredError(RuntimeError):
    pass


@dataclass(frozen=True)
class PlannedMigration:
    version: str
    migration_sql: str
    content_hash: str
    non_transactional: bool


def _accepted_hashes(version: str, content_hash: str) -> set[str]:
    return set(COMPATIBLE_MIGRATION_HASHES.get(version, {content_hash}))


async def build_migration_plan(
    connection: asyncpg.Connection,
    migration_files: list[Path],
    *,
    allow_non_transactional: bool,
) -> list[PlannedMigration]:
    records = await load_migration_records(connection)
    pending: list[PlannedMigration] = []
    incomplete_versions: list[str] = []
    for migration_file in migration_files:
        sql = migration_file.read_text(encoding="utf-8")
        content_hash = migration_content_hash(sql)
        accepted_hashes = _accepted_hashes(migration_file.name, content_hash)
        non_transactional, migration_sql = parse_migration_sql(sql)
        record = records.get(migration_file.name)
        if record is not None and record.status == "applied":
            if record.content_hash is None:
                trusted_hash = TRUSTED_MIGRATION_HASHES.get(migration_file.name)
                if trusted_hash is None:
                    raise MissingMigrationHashError(migration_file.name)
                if trusted_hash != content_hash:
                    raise MigrationHashMismatchError(migration_file.name)
                await backfill_migration_hash(
                    connection, migration_file.name, trusted_hash
                )
                continue
            if record.content_hash not in accepted_hashes:
                raise MigrationHashMismatchError(migration_file.name)
            continue
        if record is not None and record.content_hash is None:
            raise MissingMigrationHashError(migration_file.name)
        if record is not None and record.content_hash not in accepted_hashes:
            raise MigrationHashMismatchError(migration_file.name)
        if non_transactional and not allow_non_transactional:
            if record is not None:
                incomplete_versions.append(migration_file.name)
                continue
            if migration_file.name not in STARTUP_NON_TRANSACTIONAL_MIGRATIONS:
                raise NonTransactionalMigrationMaintenanceRequiredError(
                    migration_file.name,
                )
        pending.append(
            PlannedMigration(
                version=migration_file.name,
                migration_sql=migration_sql,
                content_hash=content_hash,
                non_transactional=non_transactional,
            )
        )
    if incomplete_versions:
        versions = ", ".join(sorted(incomplete_versions))
        raise IncompleteNonTransactionalMigrationError(
            "maintenance mode is required to resume incomplete "
            f"non-transactional migrations: {versions}"
        )
    return pending
