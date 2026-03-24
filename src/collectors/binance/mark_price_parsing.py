"""Parser for Binance Futures markPrice @1s WebSocket messages.

Binance combined stream ``data`` payload for ``<symbol>@markPrice@1s``:

.. code-block:: json

    {
        "e": "markPriceUpdate",
        "E": 1562305380000,
        "s": "BTCUSDT",
        "p": "11794.15000000",
        "i": "11784.62659091",
        "P": "11784.25641265",
        "r": "0.00038167",
        "T": 1562306400000
    }

Maps to ``bn_mark_price`` Bronze table columns:
``time, source, symbol, mark_price, index_price, estimated_settle_price,
premium, funding_rate, next_funding_time, event_hash, payload``.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime

from .binance_field_helpers import (
    MAX_TIME_MS,
    ms_to_datetime,
    optional_nonneg_float,
    require_float,
    require_nonneg_float,
    require_nonneg_int,
    require_positive_int,
    require_str,
)

SOURCE_ID = "bn_ws_mark_price"
ALLOWED_SYMBOLS = ("ETHUSDT", "BTCUSDT")
_CTX = "markPrice"


@dataclass(frozen=True, slots=True)
class BinanceMarkPriceRecord:
    """Parsed Binance markPrice record ready for DB insertion."""

    time: datetime
    source: str
    symbol: str
    mark_price: float
    index_price: float
    estimated_settle_price: float | None
    premium: float
    funding_rate: float
    next_funding_time: datetime | None
    event_hash: str
    payload: str

    def as_copy_row(self) -> tuple[object, ...]:
        return (
            self.time,
            self.source,
            self.symbol,
            self.mark_price,
            self.index_price,
            self.estimated_settle_price,
            self.premium,
            self.funding_rate,
            self.next_funding_time,
            self.event_hash,
            self.payload,
        )


def parse_binance_mark_price(
    data: object,
    *,
    source: str = SOURCE_ID,
    allowed_symbols: tuple[str, ...] = ALLOWED_SYMBOLS,
) -> BinanceMarkPriceRecord:
    """Parse a single Binance markPriceUpdate ``data`` payload.

    Raises ``ValueError`` on invalid or unexpected fields.
    """
    if not isinstance(data, Mapping):
        raise ValueError("Binance markPrice payload must be a mapping")
    symbol = require_str(data, "s", _CTX).upper()
    if symbol not in allowed_symbols:
        raise ValueError(f"Unexpected Binance markPrice symbol: {symbol}")
    event_time_ms = require_positive_int(data, "E", _CTX, maximum=MAX_TIME_MS)
    mark_price = require_nonneg_float(data, "p", _CTX)
    index_price = require_nonneg_float(data, "i", _CTX)
    estimated_settle_price = optional_nonneg_float(data, "P", _CTX)
    funding_rate = require_float(data, "r", _CTX)
    next_funding_ms = require_nonneg_int(data, "T", _CTX, maximum=MAX_TIME_MS)
    next_funding_time = (
        ms_to_datetime(next_funding_ms, _CTX) if next_funding_ms > 0 else None
    )
    premium = mark_price - index_price
    payload_str = json.dumps(dict(data), separators=(",", ":"), sort_keys=True)
    event_hash = _compute_event_hash(source, symbol, event_time_ms, payload_str)
    return BinanceMarkPriceRecord(
        time=ms_to_datetime(event_time_ms, _CTX),
        source=source,
        symbol=symbol,
        mark_price=mark_price,
        index_price=index_price,
        estimated_settle_price=estimated_settle_price,
        premium=premium,
        funding_rate=funding_rate,
        next_funding_time=next_funding_time,
        event_hash=event_hash,
        payload=payload_str,
    )


def _compute_event_hash(
    source: str,
    symbol: str,
    event_time_ms: int,
    payload: str,
) -> str:
    """Deterministic SHA-256 hash for replay deduplication."""
    raw = f"{source}|{symbol}|{event_time_ms}|{payload}"
    return hashlib.sha256(raw.encode()).hexdigest()
