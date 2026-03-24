"""Shared field extraction helpers for Binance WebSocket message parsers.

Provides strict validation functions for extracting typed values from
Binance combined stream ``data`` payloads. Each function accepts a
``context`` string (e.g. "markPrice", "aggTrade") for clear error messages.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta

MAX_BIGINT = 9_223_372_036_854_775_807
MAX_TIME_MS = 253_402_300_799_999
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def require_str(values: Mapping[str, object], key: str, context: str) -> str:
    value = values.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"Binance {context} field {key} must be a non-empty string")
    return value


def require_bool(values: Mapping[str, object], key: str, context: str) -> bool:
    value = values.get(key)
    if not isinstance(value, bool):
        raise ValueError(f"Binance {context} field {key} must be a boolean")
    return value


def require_float(values: Mapping[str, object], key: str, context: str) -> float:
    value = values.get(key)
    if isinstance(value, bool) or value is None:
        raise ValueError(f"Binance {context} field {key} must be numeric")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"Binance {context} field {key} must be numeric") from error
    if not math.isfinite(parsed):
        raise ValueError(f"Binance {context} field {key} must be finite")
    return parsed


def require_positive_float(
    values: Mapping[str, object], key: str, context: str
) -> float:
    parsed = require_float(values, key, context)
    if parsed <= 0:
        raise ValueError(f"Binance {context} field {key} must be positive")
    return parsed


def require_nonneg_float(values: Mapping[str, object], key: str, context: str) -> float:
    parsed = require_float(values, key, context)
    if parsed < 0:
        raise ValueError(f"Binance {context} field {key} must be non-negative")
    return parsed


def optional_nonneg_float(
    values: Mapping[str, object], key: str, context: str
) -> float | None:
    """Parse a float field that may be absent or empty string."""
    value = values.get(key)
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError(f"Binance {context} field {key} must be numeric")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"Binance {context} field {key} must be numeric") from error
    if not math.isfinite(parsed):
        raise ValueError(f"Binance {context} field {key} must be finite")
    if parsed < 0:
        raise ValueError(f"Binance {context} field {key} must be non-negative")
    return parsed


def require_int(values: Mapping[str, object], key: str, context: str) -> int:
    value = values.get(key)
    if isinstance(value, bool) or value is None:
        raise ValueError(f"Binance {context} field {key} must be an integer")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not value.is_integer():
            raise ValueError(f"Binance {context} field {key} must be an integer")
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            pass
        raise ValueError(f"Binance {context} field {key} must be an integer")
    raise ValueError(f"Binance {context} field {key} must be an integer")


def require_positive_int(
    values: Mapping[str, object],
    key: str,
    context: str,
    *,
    maximum: int = MAX_BIGINT,
) -> int:
    parsed = require_int(values, key, context)
    if parsed <= 0:
        raise ValueError(f"Binance {context} field {key} must be positive")
    if parsed > maximum:
        raise ValueError(f"Binance {context} field {key} is out of range")
    return parsed


def require_nonneg_int(
    values: Mapping[str, object],
    key: str,
    context: str,
    *,
    maximum: int = MAX_BIGINT,
) -> int:
    parsed = require_int(values, key, context)
    if parsed < 0:
        raise ValueError(f"Binance {context} field {key} must be non-negative")
    if parsed > maximum:
        raise ValueError(f"Binance {context} field {key} is out of range")
    return parsed


def ms_to_datetime(ms: int, context: str) -> datetime:
    try:
        return EPOCH + timedelta(milliseconds=ms)
    except OverflowError as error:
        raise ValueError(
            f"Binance {context} timestamp must be a valid UTC timestamp"
        ) from error
