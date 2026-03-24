"""Parser for Binance Futures depth20 @100ms WebSocket messages.

Binance combined stream ``data`` payload for ``<symbol>@depth20@100ms``:

.. code-block:: json

    {
        "lastUpdateId": 160,
        "E": 1615560345789,
        "T": 1615560345680,
        "bids": [["7403.89", "0.002"], ["7403.88", "1.500"]],
        "asks": [["7405.96", "3.340"], ["7406.63", "4.707"]]
    }

Note: the partial book depth payload does NOT include a symbol field.
The symbol must be provided externally from the stream name.

Maps to ``bn_depth`` Bronze table columns:
``time, source, symbol, first_update_id, final_update_id, last_update_id,
bids, asks, snapshot_hash, best_bid_price, best_ask_price, payload``.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime

from .binance_field_helpers import (
    MAX_TIME_MS,
    ms_to_datetime,
    require_positive_int,
)

SOURCE_ID = "bn_ws_depth"
ALLOWED_SYMBOLS = ("ETHUSDT", "BTCUSDT")
_CTX = "depth20"


@dataclass(frozen=True, slots=True)
class BinanceDepthRecord:
    """Parsed Binance partial book depth record ready for DB insertion."""

    time: datetime
    source: str
    symbol: str
    last_update_id: int
    bids: str
    asks: str
    snapshot_hash: str
    best_bid_price: float | None
    best_ask_price: float | None
    payload: str

    def as_copy_row(self) -> tuple[object, ...]:
        return (
            self.time,
            self.source,
            self.symbol,
            None,  # first_update_id — diff depth only
            None,  # final_update_id — diff depth only
            self.last_update_id,
            self.bids,
            self.asks,
            self.snapshot_hash,
            self.best_bid_price,
            self.best_ask_price,
            self.payload,
        )


def parse_binance_depth(
    data: object,
    *,
    symbol: str,
    source: str = SOURCE_ID,
    allowed_symbols: tuple[str, ...] = ALLOWED_SYMBOLS,
) -> BinanceDepthRecord:
    """Parse a single Binance partial book depth ``data`` payload.

    The ``symbol`` must be provided externally (not present in payload).
    Raises ``ValueError`` on invalid or unexpected fields.
    """
    if not isinstance(data, Mapping):
        raise ValueError("Binance depth20 payload must be a mapping")
    symbol_upper = symbol.upper()
    if symbol_upper not in allowed_symbols:
        raise ValueError(f"Unexpected Binance depth20 symbol: {symbol_upper}")
    event_time_ms = require_positive_int(data, "E", _CTX, maximum=MAX_TIME_MS)
    last_update_id = require_positive_int(data, "lastUpdateId", _CTX)
    bids = _require_levels(data, "bids")
    asks = _require_levels(data, "asks")
    best_bid = float(bids[0][0]) if bids else None
    best_ask = float(asks[0][0]) if asks else None
    bids_json = json.dumps(bids, separators=(",", ":"))
    asks_json = json.dumps(asks, separators=(",", ":"))
    payload_str = json.dumps(dict(data), separators=(",", ":"), sort_keys=True)
    snapshot_hash = _compute_snapshot_hash(
        source, symbol_upper, event_time_ms, last_update_id, payload_str
    )
    return BinanceDepthRecord(
        time=ms_to_datetime(event_time_ms, _CTX),
        source=source,
        symbol=symbol_upper,
        last_update_id=last_update_id,
        bids=bids_json,
        asks=asks_json,
        snapshot_hash=snapshot_hash,
        best_bid_price=best_bid,
        best_ask_price=best_ask,
        payload=payload_str,
    )


def _require_levels(data: Mapping[str, object], key: str) -> list[list[object]]:
    """Validate and extract price levels array from ``data[key]``."""
    raw = data.get(key)
    if not isinstance(raw, (list, tuple)):
        raise ValueError(f"Binance depth20 field {key} must be an array")
    validated: list[list[object]] = []
    for i, level in enumerate(raw):
        if not isinstance(level, (list, tuple)) or len(level) != 2:
            raise ValueError(f"Binance depth20 {key}[{i}] must be a [price, qty] pair")
        _validate_nonneg_float(level[0], f"{key}[{i}].price")
        _validate_nonneg_float(level[1], f"{key}[{i}].qty")
        validated.append(list(level))
    return validated


def _validate_nonneg_float(value: object, field: str) -> float:
    """Parse and validate a non-negative finite float."""
    if isinstance(value, bool):
        raise ValueError(f"Binance depth20 {field} must be numeric")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"Binance depth20 {field} must be numeric") from error
    if not math.isfinite(parsed):
        raise ValueError(f"Binance depth20 {field} must be finite")
    if parsed < 0:
        raise ValueError(f"Binance depth20 {field} must be non-negative")
    return parsed


def _compute_snapshot_hash(
    source: str,
    symbol: str,
    event_time_ms: int,
    last_update_id: int,
    payload: str,
) -> str:
    """Deterministic SHA-256 hash for replay deduplication."""
    raw = f"{source}|{symbol}|{event_time_ms}|{last_update_id}|{payload}"
    return hashlib.sha256(raw.encode()).hexdigest()
