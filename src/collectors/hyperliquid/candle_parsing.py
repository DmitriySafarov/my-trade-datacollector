from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
MAX_TIME_MS = 253_402_300_799_999

VALID_INTERVALS: tuple[str, ...] = (
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
    "8h",
    "12h",
    "1d",
)


@dataclass(frozen=True, slots=True)
class HyperliquidCandleRecord:
    time: datetime
    source: str
    coin: str
    interval: str
    open_time: datetime
    close_time: datetime | None
    open: float
    high: float
    low: float
    close: float
    volume: float
    trades_count: int | None
    is_closed: bool
    event_hash: str
    payload: str

    def as_copy_row(self) -> tuple[object, ...]:
        return (
            self.time,
            self.source,
            self.coin,
            self.interval,
            self.open_time,
            self.close_time,
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume,
            self.trades_count,
            self.is_closed,
            self.event_hash,
            self.payload,
        )


def parse_hyperliquid_candle(
    candle: object,
    *,
    source: str,
    allowed_coins: Sequence[str],
) -> HyperliquidCandleRecord:
    if not isinstance(candle, Mapping):
        raise ValueError("Hyperliquid candle payload must be an object")
    coin = _require_str(candle, "s").upper()
    if coin not in allowed_coins:
        raise ValueError(f"Unexpected Hyperliquid candle coin: {coin}")
    interval = _require_str(candle, "i")
    if interval not in VALID_INTERVALS:
        raise ValueError(f"Unsupported Hyperliquid candle interval: {interval}")
    open_time_ms = _require_positive_time(candle, "t")
    close_time_ms = _require_positive_time(candle, "T")
    open_price = _require_positive_float(candle, "o")
    high_price = _require_positive_float(candle, "h")
    low_price = _require_positive_float(candle, "l")
    close_price = _require_positive_float(candle, "c")
    volume = _require_non_negative_float(candle, "v")
    trades_count = _require_optional_non_negative_int(candle, "n")
    if high_price < low_price:
        raise ValueError("Hyperliquid candle high must be >= low")
    if open_price < low_price or open_price > high_price:
        raise ValueError("Hyperliquid candle open must be within [low, high]")
    if close_price < low_price or close_price > high_price:
        raise ValueError("Hyperliquid candle close must be within [low, high]")
    if close_time_ms < open_time_ms:
        raise ValueError("Hyperliquid candle close_time must be >= open_time")
    open_time = _ms_to_datetime(open_time_ms)
    close_time = _ms_to_datetime(close_time_ms)
    # Hyperliquid WS candle messages do not include an explicit is_closed field.
    # Heuristic: an open (in-progress) candle has T == t; a closed candle has T > t.
    # Verified against live API samples — open candles repeat the open_time as close_time.
    is_closed = close_time_ms != open_time_ms
    candle_dict = dict(candle)
    payload = json.dumps(candle_dict, separators=(",", ":"))
    canonical_payload = json.dumps(candle_dict, separators=(",", ":"), sort_keys=True)
    return HyperliquidCandleRecord(
        time=datetime.now(UTC),
        source=source,
        coin=coin,
        interval=interval,
        open_time=open_time,
        close_time=close_time,
        open=open_price,
        high=high_price,
        low=low_price,
        close=close_price,
        volume=volume,
        trades_count=trades_count,
        is_closed=is_closed,
        event_hash=_event_hash(canonical_payload),
        payload=payload,
    )


def _event_hash(canonical_payload: str) -> str:
    return hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()


def _ms_to_datetime(time_ms: int) -> datetime:
    try:
        return EPOCH + timedelta(milliseconds=time_ms)
    except OverflowError as error:
        raise ValueError("Hyperliquid candle timestamp is out of range") from error


def _require_str(values: Mapping[str, object], key: str) -> str:
    value = values.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"Hyperliquid candle field {key} must be a non-empty string")
    return value


def _require_float(values: Mapping[str, object], key: str) -> float:
    value = values.get(key)
    if isinstance(value, bool) or value is None:
        raise ValueError(f"Hyperliquid candle field {key} must be numeric")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"Hyperliquid candle field {key} must be numeric") from error
    if not math.isfinite(parsed):
        raise ValueError(f"Hyperliquid candle field {key} must be finite")
    return parsed


def _require_positive_float(values: Mapping[str, object], key: str) -> float:
    parsed = _require_float(values, key)
    if parsed <= 0:
        raise ValueError(f"Hyperliquid candle field {key} must be positive")
    return parsed


def _require_non_negative_float(values: Mapping[str, object], key: str) -> float:
    parsed = _require_float(values, key)
    if parsed < 0:
        raise ValueError(f"Hyperliquid candle field {key} must be non-negative")
    return parsed


def _require_positive_time(values: Mapping[str, object], key: str) -> int:
    value = values.get(key)
    if isinstance(value, bool) or value is None:
        raise ValueError(f"Hyperliquid candle field {key} must be a timestamp")
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, float):
        if not value.is_integer():
            raise ValueError(f"Hyperliquid candle field {key} must be an integer")
        parsed = int(value)
    else:
        raise ValueError(f"Hyperliquid candle field {key} must be a timestamp")
    if parsed <= 0:
        raise ValueError(f"Hyperliquid candle field {key} must be positive")
    if parsed > MAX_TIME_MS:
        raise ValueError(
            f"Hyperliquid candle field {key} must be a valid UTC timestamp"
        )
    return parsed


def _require_optional_non_negative_int(
    values: Mapping[str, object], key: str
) -> int | None:
    value = values.get(key)
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"Hyperliquid candle field {key} must be an integer")
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, float):
        if not value.is_integer():
            raise ValueError(f"Hyperliquid candle field {key} must be an integer")
        parsed = int(value)
    else:
        raise ValueError(f"Hyperliquid candle field {key} must be an integer")
    if parsed < 0:
        raise ValueError(f"Hyperliquid candle field {key} must be non-negative")
    return parsed
