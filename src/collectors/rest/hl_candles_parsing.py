"""Parse Hyperliquid REST candleSnapshot responses into hl_rest_candles rows.

Reuses the validated WS candle parser and adapts the record shape
to match the hl_rest_candles Bronze table (no is_closed, no event_hash,
time = open_time per CHECK constraint).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

from src.collectors.hyperliquid.candle_parsing import parse_hyperliquid_candle

SOURCE_ID = "hl_rest_candles"


@dataclass(frozen=True, slots=True)
class RestCandleRecord:
    """Row matching hl_rest_candles Bronze table schema."""

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
    payload: str

    def as_row(self) -> tuple[object, ...]:
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
            self.payload,
        )


def parse_rest_candle(
    candle: object,
    *,
    allowed_coins: Sequence[str],
) -> RestCandleRecord | None:
    """Parse a single candle from Hyperliquid REST candleSnapshot response.

    Delegates field validation to the shared WS candle parser, then
    adapts the result for the REST-specific table layout where
    ``time = open_time`` (no event_hash or is_closed columns).

    Returns ``None`` for open (in-progress) candles.  The ``hl_rest_candles``
    table uses ``ON CONFLICT DO NOTHING`` — an open candle inserted once
    would never be updated with final OHLCV values.  Only closed candles
    are suitable for the append-only gap-fill table.  Open candles are
    expected on every poll (the current interval is always in-progress),
    so they are silently skipped rather than raising.
    """
    ws_record = parse_hyperliquid_candle(
        candle,
        source=SOURCE_ID,
        allowed_coins=allowed_coins,
    )
    if not ws_record.is_closed:
        return None
    return RestCandleRecord(
        time=ws_record.open_time,
        source=SOURCE_ID,
        coin=ws_record.coin,
        interval=ws_record.interval,
        open_time=ws_record.open_time,
        close_time=ws_record.close_time,
        open=ws_record.open,
        high=ws_record.high,
        low=ws_record.low,
        close=ws_record.close,
        volume=ws_record.volume,
        trades_count=ws_record.trades_count,
        payload=ws_record.payload,
    )
