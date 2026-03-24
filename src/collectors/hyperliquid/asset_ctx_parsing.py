from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass(frozen=True, slots=True)
class HyperliquidAssetCtxRecord:
    time: datetime
    source: str
    coin: str
    funding: float
    open_interest: float
    mark_price: float
    oracle_price: float
    mid_price: float | None
    premium: float
    buy_impact_price: float | None
    sell_impact_price: float | None
    day_notional_volume: float
    prev_day_price: float
    event_hash: str
    payload: str

    def as_copy_row(self) -> tuple[object, ...]:
        return (
            self.time,
            self.source,
            self.coin,
            self.funding,
            self.open_interest,
            self.mark_price,
            self.oracle_price,
            self.mid_price,
            self.premium,
            self.buy_impact_price,
            self.sell_impact_price,
            self.day_notional_volume,
            self.prev_day_price,
            self.event_hash,
            self.payload,
        )


def parse_hyperliquid_asset_ctx(
    asset_ctx: object,
    *,
    source: str,
    allowed_coins: Sequence[str],
) -> HyperliquidAssetCtxRecord:
    if not isinstance(asset_ctx, Mapping):
        raise ValueError("Hyperliquid activeAssetCtx payload must be an object")
    coin = _require_str(asset_ctx, "coin").upper()
    if coin not in allowed_coins:
        raise ValueError(f"Unexpected Hyperliquid activeAssetCtx coin: {coin}")
    ctx = _require_mapping(asset_ctx, "ctx")
    payload = json.dumps(dict(asset_ctx), separators=(",", ":"))
    canonical_payload = json.dumps(
        dict(asset_ctx), separators=(",", ":"), sort_keys=True
    )
    buy_impact_price, sell_impact_price = _require_impact_prices(ctx.get("impactPxs"))
    return HyperliquidAssetCtxRecord(
        time=datetime.now(UTC),
        source=source,
        coin=coin,
        funding=_require_float(ctx, "funding"),
        open_interest=_require_non_negative_float(ctx, "openInterest"),
        mark_price=_require_positive_float(ctx, "markPx"),
        oracle_price=_require_positive_float(ctx, "oraclePx"),
        mid_price=_require_optional_positive_float(ctx, "midPx"),
        premium=_require_float(ctx, "premium"),
        buy_impact_price=buy_impact_price,
        sell_impact_price=sell_impact_price,
        day_notional_volume=_require_non_negative_float(ctx, "dayNtlVlm"),
        prev_day_price=_require_positive_float(ctx, "prevDayPx"),
        event_hash=_event_hash(canonical_payload),
        payload=payload,
    )


def _event_hash(canonical_payload: str) -> str:
    return hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()


def _require_mapping(values: Mapping[str, object], key: str) -> Mapping[str, object]:
    value = values.get(key)
    if not isinstance(value, Mapping):
        raise ValueError(f"Hyperliquid activeAssetCtx field {key} must be an object")
    return value


def _require_str(values: Mapping[str, object], key: str) -> str:
    value = values.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(
            f"Hyperliquid activeAssetCtx field {key} must be a non-empty string"
        )
    return value


def _require_float(values: Mapping[str, object], key: str) -> float:
    value = values.get(key)
    if isinstance(value, bool) or value is None:
        raise ValueError(f"Hyperliquid activeAssetCtx field {key} must be numeric")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(
            f"Hyperliquid activeAssetCtx field {key} must be numeric"
        ) from error
    if not math.isfinite(parsed):
        raise ValueError(f"Hyperliquid activeAssetCtx field {key} must be finite")
    return parsed


def _require_positive_float(values: Mapping[str, object], key: str) -> float:
    parsed = _require_float(values, key)
    if parsed <= 0:
        raise ValueError(f"Hyperliquid activeAssetCtx field {key} must be positive")
    return parsed


def _require_non_negative_float(values: Mapping[str, object], key: str) -> float:
    parsed = _require_float(values, key)
    if parsed < 0:
        raise ValueError(f"Hyperliquid activeAssetCtx field {key} must be non-negative")
    return parsed


def _require_optional_positive_float(
    values: Mapping[str, object], key: str
) -> float | None:
    value = values.get(key)
    if value is None:
        return None
    parsed = _require_float(values, key)
    if parsed <= 0:
        raise ValueError(f"Hyperliquid activeAssetCtx field {key} must be positive")
    return parsed


def _require_impact_prices(value: object) -> tuple[float | None, float | None]:
    if value is None:
        return None, None
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError("Hyperliquid activeAssetCtx field impactPxs must be a pair")
    prices = list(value)
    if len(prices) != 2:
        raise ValueError("Hyperliquid activeAssetCtx field impactPxs must be a pair")
    buy_price = _require_positive_number(prices[0], key="impactPxs[0]")
    sell_price = _require_positive_number(prices[1], key="impactPxs[1]")
    return buy_price, sell_price


def _require_positive_number(value: object, *, key: str) -> float:
    if isinstance(value, bool) or value is None:
        raise ValueError(f"Hyperliquid activeAssetCtx field {key} must be numeric")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(
            f"Hyperliquid activeAssetCtx field {key} must be numeric"
        ) from error
    if not math.isfinite(parsed) or parsed <= 0:
        raise ValueError(f"Hyperliquid activeAssetCtx field {key} must be positive")
    return parsed
