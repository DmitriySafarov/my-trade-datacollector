"""Parse Hyperliquid REST fundingHistory responses into hl_rest_funding rows.

Each response record contains ``time``, ``funding``, and ``premium``.
The ``mark_price`` and ``oracle_price`` fields are not available in the
fundingHistory endpoint — they are stored as NULL.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

SOURCE_ID = "hl_rest_funding"


@dataclass(frozen=True, slots=True)
class RestFundingRecord:
    """Row matching hl_rest_funding Bronze table schema."""

    time: datetime
    source: str
    coin: str
    funding: float
    premium: float
    mark_price: float | None
    oracle_price: float | None
    payload: str

    def as_row(self) -> tuple[object, ...]:
        return (
            self.time,
            self.source,
            self.coin,
            self.funding,
            self.premium,
            self.mark_price,
            self.oracle_price,
            self.payload,
        )


def parse_rest_funding(
    record: object,
    *,
    coin: str,
    allowed_coins: Sequence[str],
) -> RestFundingRecord:
    """Parse a single record from Hyperliquid REST fundingHistory response.

    The ``coin`` parameter is passed explicitly because the API response
    does not include it — the coin is implicit from the request.

    ``allowed_coins`` is a belt-and-suspenders guard: the caller always
    passes a coin from the same set, so the check is vacuously true in
    normal operation.  It exists to catch bugs in future refactoring
    where coin provenance may change.

    Raises ``ValueError`` on invalid data.
    """
    if not isinstance(record, Mapping):
        raise ValueError("Hyperliquid fundingHistory record must be an object")

    coin_upper = coin.upper()
    if coin_upper not in allowed_coins:
        raise ValueError(f"Unexpected Hyperliquid funding coin: {coin_upper}")

    time_ms = record.get("time")
    if not isinstance(time_ms, (int, float)) or isinstance(time_ms, bool):
        raise ValueError("Hyperliquid fundingHistory field time must be numeric")
    if time_ms <= 0:
        raise ValueError("Hyperliquid fundingHistory field time must be positive")

    funding = _require_float(record, "funding")
    premium = _require_float(record, "premium")

    event_time = datetime.fromtimestamp(time_ms / 1000.0, tz=UTC)
    payload = json.dumps(dict(record), separators=(",", ":"))

    return RestFundingRecord(
        time=event_time,
        source=SOURCE_ID,
        coin=coin_upper,
        funding=funding,
        premium=premium,
        mark_price=None,
        oracle_price=None,
        payload=payload,
    )


def _require_float(values: Mapping[str, object], key: str) -> float:
    """Extract and validate a finite float from a string or numeric value."""
    value = values.get(key)
    if isinstance(value, bool) or value is None:
        raise ValueError(f"Hyperliquid fundingHistory field {key} must be numeric")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(
            f"Hyperliquid fundingHistory field {key} must be numeric"
        ) from error
    if not math.isfinite(parsed):
        raise ValueError(f"Hyperliquid fundingHistory field {key} must be finite")
    return parsed
