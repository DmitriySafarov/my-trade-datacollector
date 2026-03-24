"""Parser for Binance Futures aggTrade WebSocket messages.

Binance combined stream ``data`` payload for ``<symbol>@aggTrade``:

.. code-block:: json

    {
        "e": "aggTrade",
        "E": 1672515782136,
        "s": "ETHUSDT",
        "a": 12345678,
        "p": "3021.50",
        "q": "1.250",
        "f": 100,
        "l": 105,
        "T": 1672515782136,
        "m": true
    }

Maps to ``bn_agg_trades`` Bronze table columns:
``time, source, symbol, trade_id, agg_trade_id, first_trade_id,
last_trade_id, price, qty, is_buyer_maker, is_best_match, payload``.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

SOURCE_ID = "bn_ws_agg_trades"
ALLOWED_SYMBOLS = ("ETHUSDT", "BTCUSDT")
MAX_BIGINT = 9_223_372_036_854_775_807
MAX_TIME_MS = 253_402_300_799_999
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class BinanceAggTradeRecord:
    """Parsed Binance aggTrade record ready for DB insertion."""

    time: datetime
    source: str
    symbol: str
    agg_trade_id: int
    first_trade_id: int
    last_trade_id: int
    price: float
    qty: float
    is_buyer_maker: bool
    payload: str

    def as_copy_row(self) -> tuple[object, ...]:
        return (
            self.time,
            self.source,
            self.symbol,
            None,  # trade_id — not present in aggTrade
            self.agg_trade_id,
            self.first_trade_id,
            self.last_trade_id,
            self.price,
            self.qty,
            self.is_buyer_maker,
            None,  # is_best_match — not in Futures aggTrade
            self.payload,
        )


def parse_binance_agg_trade(
    data: object,
    *,
    source: str = SOURCE_ID,
    allowed_symbols: tuple[str, ...] = ALLOWED_SYMBOLS,
) -> BinanceAggTradeRecord:
    """Parse a single Binance aggTrade ``data`` payload.

    Raises ``ValueError`` on invalid or unexpected fields.
    """
    if not isinstance(data, Mapping):
        raise ValueError("Binance aggTrade payload must be a mapping")
    symbol = _require_str(data, "s").upper()
    if symbol not in allowed_symbols:
        raise ValueError(f"Unexpected Binance aggTrade symbol: {symbol}")
    trade_time_ms = _require_positive_int(data, "T", maximum=MAX_TIME_MS)
    return BinanceAggTradeRecord(
        time=_ms_to_datetime(trade_time_ms),
        source=source,
        symbol=symbol,
        agg_trade_id=_require_positive_int(data, "a", maximum=MAX_BIGINT),
        first_trade_id=_require_nonneg_int(data, "f", maximum=MAX_BIGINT),
        last_trade_id=_require_nonneg_int(data, "l", maximum=MAX_BIGINT),
        price=_require_positive_float(data, "p"),
        qty=_require_positive_float(data, "q"),
        is_buyer_maker=_require_bool(data, "m"),
        payload=json.dumps(dict(data), separators=(",", ":"), sort_keys=True),
    )


# ---------------------------------------------------------------------------
# Field extraction helpers
# ---------------------------------------------------------------------------


def _require_str(values: Mapping[str, object], key: str) -> str:
    value = values.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"Binance aggTrade field {key} must be a non-empty string")
    return value


def _require_bool(values: Mapping[str, object], key: str) -> bool:
    value = values.get(key)
    if not isinstance(value, bool):
        raise ValueError(f"Binance aggTrade field {key} must be a boolean")
    return value


def _require_float(values: Mapping[str, object], key: str) -> float:
    value = values.get(key)
    if isinstance(value, bool) or value is None:
        raise ValueError(f"Binance aggTrade field {key} must be numeric")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"Binance aggTrade field {key} must be numeric") from error
    if not math.isfinite(parsed):
        raise ValueError(f"Binance aggTrade field {key} must be finite")
    return parsed


def _require_positive_float(values: Mapping[str, object], key: str) -> float:
    parsed = _require_float(values, key)
    if parsed <= 0:
        raise ValueError(f"Binance aggTrade field {key} must be positive")
    return parsed


def _require_int(values: Mapping[str, object], key: str) -> int:
    value = values.get(key)
    if isinstance(value, bool) or value is None:
        raise ValueError(f"Binance aggTrade field {key} must be an integer")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not value.is_integer():
            raise ValueError(f"Binance aggTrade field {key} must be an integer")
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            pass
        raise ValueError(f"Binance aggTrade field {key} must be an integer")
    raise ValueError(f"Binance aggTrade field {key} must be an integer")


def _require_positive_int(
    values: Mapping[str, object],
    key: str,
    *,
    maximum: int = MAX_BIGINT,
) -> int:
    parsed = _require_int(values, key)
    if parsed <= 0:
        raise ValueError(f"Binance aggTrade field {key} must be positive")
    if parsed > maximum:
        raise ValueError(f"Binance aggTrade field {key} is out of range")
    return parsed


def _require_nonneg_int(
    values: Mapping[str, object],
    key: str,
    *,
    maximum: int = MAX_BIGINT,
) -> int:
    parsed = _require_int(values, key)
    if parsed < 0:
        raise ValueError(f"Binance aggTrade field {key} must be non-negative")
    if parsed > maximum:
        raise ValueError(f"Binance aggTrade field {key} is out of range")
    return parsed


def _ms_to_datetime(ms: int) -> datetime:
    try:
        return EPOCH + timedelta(milliseconds=ms)
    except OverflowError as error:
        raise ValueError(
            "Binance aggTrade field T must be a valid UTC timestamp"
        ) from error
