from __future__ import annotations

from datetime import datetime, timezone


HL_REST_FUNDING_MUTATIONS = (
    """
    UPDATE hl_rest_funding
    SET funding = 0.02
    WHERE source = 'hl_rest_funding'
      AND coin = 'ETH'
    """,
    """
    DELETE FROM hl_rest_funding
    WHERE source = 'hl_rest_funding'
      AND coin = 'ETH'
    """,
    "TRUNCATE hl_rest_funding",
)
HL_REST_FUNDING_TRIGGER_NAMES = {
    "hl_rest_funding_reject_mutation_before_change",
    "hl_rest_funding_reject_truncate_before_truncate",
}


def utc(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def immutability_trigger_pairs(table_names: set[str]) -> set[tuple[str, str]]:
    return {
        (table, f"{table}_reject_mutation_before_change") for table in table_names
    } | {(table, f"{table}_reject_truncate_before_truncate") for table in table_names}


def trigger_pairs(rows, trigger_names):
    return {
        (row["table_name"], row["tgname"])
        for row in rows
        if row["tgname"] in trigger_names
    }
