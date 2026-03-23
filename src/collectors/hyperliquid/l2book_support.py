from __future__ import annotations

import math
import re
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation


INTEGER_TEXT = re.compile(r"[+-]?\d+")
MAX_TIME_MS = 253_402_300_799_999
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
LEVEL_DEPTH = 20


def decimal_to_float(value: Decimal, *, key: str) -> float:
    float_value = float(value)
    if not math.isfinite(float_value) or (value != 0 and float_value == 0.0):
        raise ValueError(
            f"Hyperliquid l2Book field {key} must be representable as a finite float"
        )
    return float_value


def require_decimal(
    values: Mapping[str, object],
    key: str,
    *,
    positive: bool,
) -> Decimal:
    value = values.get(key)
    if isinstance(value, bool) or value is None:
        raise ValueError(f"Hyperliquid l2Book field {key} must be numeric")
    try:
        decimal = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"Hyperliquid l2Book field {key} must be numeric") from error
    if not decimal.is_finite():
        raise ValueError(f"Hyperliquid l2Book field {key} must be finite")
    if positive and decimal <= 0:
        raise ValueError(f"Hyperliquid l2Book field {key} must be positive")
    if not positive and decimal < 0:
        raise ValueError(f"Hyperliquid l2Book field {key} must be non-negative")
    decimal_to_float(decimal, key=key)
    return decimal


def decimal_text(value: Decimal) -> str:
    text = format(value.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def require_non_negative_int(values: Mapping[str, object], key: str) -> int:
    parsed = require_int(values, key)
    if parsed < 0:
        raise ValueError(f"Hyperliquid l2Book field {key} must be non-negative")
    return parsed


def require_positive_int(
    values: Mapping[str, object],
    key: str,
    *,
    maximum: int | None = None,
    maximum_message: str | None = None,
) -> int:
    parsed = require_int(values, key)
    if parsed <= 0:
        raise ValueError(f"Hyperliquid l2Book field {key} must be positive")
    if maximum is not None and parsed > maximum:
        suffix = maximum_message or "is out of range"
        raise ValueError(f"Hyperliquid l2Book field {key} {suffix}")
    return parsed


def require_int(values: Mapping[str, object], key: str) -> int:
    value = values.get(key)
    if isinstance(value, bool) or value is None:
        raise ValueError(f"Hyperliquid l2Book field {key} must be an integer")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not value.is_integer():
            raise ValueError(f"Hyperliquid l2Book field {key} must be an integer")
        return int(value)
    if isinstance(value, str):
        if INTEGER_TEXT.fullmatch(value) is None:
            raise ValueError(f"Hyperliquid l2Book field {key} must be an integer")
        return int(value)
    raise ValueError(f"Hyperliquid l2Book field {key} must be an integer")


def parse_time(time_ms: int) -> datetime:
    try:
        return EPOCH + timedelta(milliseconds=time_ms)
    except OverflowError as error:
        raise ValueError(
            "Hyperliquid l2Book field time must be a valid UTC timestamp"
        ) from error


def require_str(values: Mapping[str, object], key: str) -> str:
    value = values.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"Hyperliquid l2Book field {key} must be a non-empty string")
    return value
