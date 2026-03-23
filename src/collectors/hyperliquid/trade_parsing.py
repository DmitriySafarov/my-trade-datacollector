from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


INTEGER_TEXT = re.compile(r"[+-]?\d+")
MAX_BIGINT = 9_223_372_036_854_775_807
MAX_TIME_MS = 253_402_300_799_999
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class HyperliquidTradeRecord:
    time: datetime
    source: str
    coin: str
    side: str
    price: float
    size: float
    hash: str
    tid: int
    users: str
    payload: str

    def as_copy_row(self) -> tuple[object, ...]:
        return (
            self.time,
            self.source,
            self.coin,
            self.side,
            self.price,
            self.size,
            self.hash,
            self.tid,
            self.users,
            self.payload,
        )


def parse_hyperliquid_trade(
    trade: object,
    *,
    source: str,
    allowed_coins: Sequence[str],
) -> HyperliquidTradeRecord:
    if not isinstance(trade, Mapping):
        raise ValueError("Hyperliquid trade payload must be an object")
    coin = _require_str(trade, "coin").upper()
    if coin not in allowed_coins:
        raise ValueError(f"Unexpected Hyperliquid trade coin: {coin}")
    side = _require_str(trade, "side")
    if side not in {"A", "B"}:
        raise ValueError(f"Unexpected Hyperliquid trade side: {side}")
    users = json.dumps(_require_users(trade.get("users")))
    time_ms = _require_positive_int(
        trade,
        "time",
        maximum=MAX_TIME_MS,
        maximum_message="must be a valid UTC timestamp",
    )
    return HyperliquidTradeRecord(
        time=_parse_trade_time(time_ms),
        source=source,
        coin=coin,
        side=side,
        price=_require_float(trade, "px"),
        size=_require_float(trade, "sz"),
        hash=_require_str(trade, "hash"),
        tid=_require_positive_int(
            trade,
            "tid",
            maximum=MAX_BIGINT,
            maximum_message="must fit PostgreSQL BIGINT",
        ),
        users=users,
        payload=json.dumps(dict(trade), separators=(",", ":"), sort_keys=True),
    )


def _require_float(values: Mapping[str, object], key: str) -> float:
    value = values.get(key)
    if isinstance(value, bool) or value is None:
        raise ValueError(f"Hyperliquid trade field {key} must be numeric")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"Hyperliquid trade field {key} must be numeric") from error
    if not math.isfinite(parsed):
        raise ValueError(f"Hyperliquid trade field {key} must be finite")
    return parsed


def _require_int(values: Mapping[str, object], key: str) -> int:
    value = values.get(key)
    if isinstance(value, bool) or value is None:
        raise ValueError(f"Hyperliquid trade field {key} must be an integer")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not value.is_integer():
            raise ValueError(f"Hyperliquid trade field {key} must be an integer")
        return int(value)
    if isinstance(value, str):
        if INTEGER_TEXT.fullmatch(value) is None:
            raise ValueError(f"Hyperliquid trade field {key} must be an integer")
        return int(value)
    raise ValueError(f"Hyperliquid trade field {key} must be an integer")


def _require_positive_int(
    values: Mapping[str, object],
    key: str,
    *,
    maximum: int | None = None,
    maximum_message: str | None = None,
) -> int:
    parsed = _require_int(values, key)
    if parsed <= 0:
        raise ValueError(f"Hyperliquid trade field {key} must be positive")
    if maximum is not None and parsed > maximum:
        suffix = maximum_message or "is out of range"
        raise ValueError(f"Hyperliquid trade field {key} {suffix}")
    return parsed


def _parse_trade_time(time_ms: int) -> datetime:
    try:
        return EPOCH + timedelta(milliseconds=time_ms)
    except OverflowError as error:
        raise ValueError(
            "Hyperliquid trade field time must be a valid UTC timestamp"
        ) from error


def _require_str(values: Mapping[str, object], key: str) -> str:
    value = values.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"Hyperliquid trade field {key} must be a non-empty string")
    return value


def _require_users(value: object) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError("Hyperliquid trade users must be a pair of addresses")
    users = list(value)
    if len(users) != 2 or any(not isinstance(user, str) or not user for user in users):
        raise ValueError("Hyperliquid trade users must be a pair of addresses")
    return users
