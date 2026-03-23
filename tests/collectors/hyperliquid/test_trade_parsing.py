from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.collectors.hyperliquid.trade_parsing import parse_hyperliquid_trade


FIXTURE_PATH = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "hyperliquid"
    / "ws_trades_messages.json"
)


def _load_trade() -> dict[str, object]:
    return json.loads(FIXTURE_PATH.read_text())[0]["data"][0]


@pytest.mark.parametrize(
    ("field", "value", "match"),
    [
        ("time", 1774137600123.9, "time"),
        ("time", "1774137600123.5", "time"),
        ("tid", 91001.5, "tid"),
        ("tid", "91001.5", "tid"),
        ("time", -1, "positive"),
        ("tid", -5, "positive"),
        ("time", 253402300800000, "valid UTC timestamp"),
        ("time", 10**30, "valid UTC timestamp"),
        ("tid", 9_223_372_036_854_775_808, "BIGINT"),
        ("px", 0, "positive"),
        ("px", -5, "positive"),
        ("sz", 0, "positive"),
        ("sz", -1, "positive"),
        ("px", "NaN", "finite"),
        ("sz", "Infinity", "finite"),
    ],
)
def test_parse_hyperliquid_trade_rejects_invalid_trade_fields(
    field: str,
    value: float | int | str,
    match: str,
) -> None:
    trade = _load_trade()
    trade[field] = value

    with pytest.raises(ValueError, match=match):
        parse_hyperliquid_trade(
            trade,
            source="hl_ws_trades",
            allowed_coins=("ETH",),
        )
