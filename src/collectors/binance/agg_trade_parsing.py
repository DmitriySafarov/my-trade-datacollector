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
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime

from .binance_field_helpers import (
    MAX_BIGINT,
    MAX_TIME_MS,
    ms_to_datetime,
    require_bool,
    require_nonneg_int,
    require_positive_float,
    require_positive_int,
    require_str,
)

SOURCE_ID = "bn_ws_agg_trades"
ALLOWED_SYMBOLS = ("ETHUSDT", "BTCUSDT")
_CTX = "aggTrade"


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
    symbol = require_str(data, "s", _CTX).upper()
    if symbol not in allowed_symbols:
        raise ValueError(f"Unexpected Binance aggTrade symbol: {symbol}")
    trade_time_ms = require_positive_int(data, "T", _CTX, maximum=MAX_TIME_MS)
    return BinanceAggTradeRecord(
        time=ms_to_datetime(trade_time_ms, _CTX),
        source=source,
        symbol=symbol,
        agg_trade_id=require_positive_int(data, "a", _CTX, maximum=MAX_BIGINT),
        first_trade_id=require_nonneg_int(data, "f", _CTX, maximum=MAX_BIGINT),
        last_trade_id=require_nonneg_int(data, "l", _CTX, maximum=MAX_BIGINT),
        price=require_positive_float(data, "p", _CTX),
        qty=require_positive_float(data, "q", _CTX),
        is_buyer_maker=require_bool(data, "m", _CTX),
        payload=json.dumps(dict(data), separators=(",", ":"), sort_keys=True),
    )
