from __future__ import annotations


NON_TRANSACTIONAL_MARKER = "-- migrate: no-transaction"


def parse_migration_sql(sql: str) -> tuple[bool, str]:
    non_transactional = False
    lines: list[str] = []
    for line in sql.splitlines():
        if line.strip() == NON_TRANSACTIONAL_MARKER:
            non_transactional = True
            continue
        lines.append(line)
    return non_transactional, "\n".join(lines)
