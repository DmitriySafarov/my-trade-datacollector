from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from .l2book_support import (
    LEVEL_DEPTH,
    MAX_TIME_MS,
    decimal_to_float,
    decimal_text,
    parse_time,
    require_decimal,
    require_non_negative_int,
    require_positive_int,
    require_str,
)


@dataclass(frozen=True, slots=True)
class HyperliquidL2Level:
    px: str
    sz: str
    n: int

    def as_json(self) -> dict[str, int | str]:
        return {"n": self.n, "px": self.px, "sz": self.sz}


@dataclass(frozen=True, slots=True)
class HyperliquidL2BookRecord:
    time: datetime
    source: str
    coin: str
    bids: str
    asks: str
    snapshot_hash: str
    best_bid_price: float | None
    best_bid_size: float | None
    best_ask_price: float | None
    best_ask_size: float | None
    spread: float | None
    bid_notional: float | None
    ask_notional: float | None
    payload: str

    def as_copy_row(self) -> tuple[object, ...]:
        return (
            self.time,
            self.source,
            self.coin,
            self.bids,
            self.asks,
            self.snapshot_hash,
            self.best_bid_price,
            self.best_bid_size,
            self.best_ask_price,
            self.best_ask_size,
            self.spread,
            self.bid_notional,
            self.ask_notional,
            self.payload,
        )


def parse_hyperliquid_l2book(
    book: object,
    *,
    source: str,
    allowed_coins: Sequence[str],
) -> HyperliquidL2BookRecord:
    if not isinstance(book, Mapping):
        raise ValueError("Hyperliquid l2Book payload must be an object")
    coin = require_str(book, "coin").upper()
    if coin not in allowed_coins:
        raise ValueError(f"Unexpected Hyperliquid l2Book coin: {coin}")
    raw_levels = _require_levels(book.get("levels"))
    bids = _parse_side(raw_levels[0], side="bids")
    asks = _parse_side(raw_levels[1], side="asks")
    bids_json = _levels_json(bids)
    asks_json = _levels_json(asks)
    time_ms = require_positive_int(
        book,
        "time",
        maximum=MAX_TIME_MS,
        maximum_message="must be a valid UTC timestamp",
    )
    best_bid = bids[0] if bids else None
    best_ask = asks[0] if asks else None
    return HyperliquidL2BookRecord(
        time=parse_time(time_ms),
        source=source,
        coin=coin,
        bids=bids_json,
        asks=asks_json,
        snapshot_hash=_snapshot_hash(coin, time_ms, bids_json, asks_json),
        best_bid_price=_level_float(best_bid, "px", key="best_bid_price"),
        best_bid_size=_level_float(best_bid, "sz", key="best_bid_size"),
        best_ask_price=_level_float(best_ask, "px", key="best_ask_price"),
        best_ask_size=_level_float(best_ask, "sz", key="best_ask_size"),
        spread=_spread(best_bid, best_ask),
        bid_notional=_notional(bids, key="bid_notional"),
        ask_notional=_notional(asks, key="ask_notional"),
        payload=json.dumps(dict(book), separators=(",", ":")),
    )


def _snapshot_hash(coin: str, time_ms: int, bids_json: str, asks_json: str) -> str:
    payload = json.dumps(
        {
            "asks": json.loads(asks_json),
            "bids": json.loads(bids_json),
            "coin": coin,
            "time": time_ms,
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _levels_json(levels: list[HyperliquidL2Level]) -> str:
    return json.dumps(
        [level.as_json() for level in levels],
        separators=(",", ":"),
        sort_keys=True,
    )


def _level_float(
    level: HyperliquidL2Level | None, field: str, *, key: str
) -> float | None:
    if level is None:
        return None
    return decimal_to_float(Decimal(getattr(level, field)), key=key)


def _notional(levels: list[HyperliquidL2Level], *, key: str) -> float | None:
    if not levels:
        return None
    total = sum(
        (Decimal(level.px) * Decimal(level.sz) for level in levels),
        start=Decimal("0"),
    )
    return decimal_to_float(total, key=key)


def _spread(
    best_bid: HyperliquidL2Level | None,
    best_ask: HyperliquidL2Level | None,
) -> float | None:
    if best_bid is None or best_ask is None:
        return None
    return decimal_to_float(
        Decimal(best_ask.px) - Decimal(best_bid.px),
        key="spread",
    )


def _require_levels(value: object) -> tuple[object, object]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError("Hyperliquid l2Book field levels must contain bid/ask arrays")
    levels = list(value)
    if len(levels) != 2:
        raise ValueError("Hyperliquid l2Book field levels must contain bid/ask arrays")
    return levels[0], levels[1]


def _parse_side(value: object, *, side: str) -> list[HyperliquidL2Level]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError(f"Hyperliquid l2Book side {side} must be an array")
    levels: list[HyperliquidL2Level] = []
    for index, level in enumerate(list(value)[:LEVEL_DEPTH]):
        if not isinstance(level, Mapping):
            raise ValueError(
                f"Hyperliquid l2Book side {side} level {index} must be an object"
            )
        levels.append(
            HyperliquidL2Level(
                px=decimal_text(require_decimal(level, "px", positive=True)),
                sz=decimal_text(require_decimal(level, "sz", positive=False)),
                n=require_non_negative_int(level, "n"),
            )
        )
    return levels
