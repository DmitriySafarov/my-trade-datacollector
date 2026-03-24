"""Tests for REST candleSnapshot parsing."""

from __future__ import annotations

import pytest

from src.collectors.rest.hl_candles_parsing import (
    SOURCE_ID,
    RestCandleRecord,
    parse_rest_candle,
)

ALLOWED_COINS = ("ETH", "BTC")


def _candle(overrides: dict | None = None) -> dict:
    base = {
        "t": 1_700_000_000_000,
        "T": 1_700_003_600_000,
        "s": "ETH",
        "i": "1h",
        "o": "3450.5",
        "h": "3465.0",
        "l": "3440.0",
        "c": "3455.25",
        "v": "12345.67",
        "n": 5000,
    }
    if overrides:
        base.update(overrides)
    return base


class TestParseRestCandle:
    def test_valid_candle_produces_record(self) -> None:
        record = parse_rest_candle(_candle(), allowed_coins=ALLOWED_COINS)
        assert isinstance(record, RestCandleRecord)
        assert record.source == SOURCE_ID
        assert record.coin == "ETH"
        assert record.interval == "1h"
        assert record.time == record.open_time  # CHECK constraint
        assert record.open == 3450.5
        assert record.high == 3465.0
        assert record.low == 3440.0
        assert record.close == 3455.25
        assert record.volume == 12345.67
        assert record.trades_count == 5000

    def test_time_equals_open_time(self) -> None:
        record = parse_rest_candle(_candle(), allowed_coins=ALLOWED_COINS)
        assert record.time == record.open_time

    def test_source_is_hl_rest_candles(self) -> None:
        record = parse_rest_candle(_candle(), allowed_coins=ALLOWED_COINS)
        assert record.source == "hl_rest_candles"

    def test_btc_coin_accepted(self) -> None:
        record = parse_rest_candle(
            _candle({"s": "BTC"}),
            allowed_coins=ALLOWED_COINS,
        )
        assert record.coin == "BTC"

    def test_unknown_coin_rejected(self) -> None:
        with pytest.raises(ValueError, match="Unexpected"):
            parse_rest_candle(_candle({"s": "SOL"}), allowed_coins=ALLOWED_COINS)

    def test_invalid_interval_rejected(self) -> None:
        with pytest.raises(ValueError, match="Unsupported"):
            parse_rest_candle(_candle({"i": "99m"}), allowed_coins=ALLOWED_COINS)

    def test_high_less_than_low_rejected(self) -> None:
        with pytest.raises(ValueError, match="high must be >= low"):
            parse_rest_candle(
                _candle({"h": "3400.0", "l": "3440.0"}),
                allowed_coins=ALLOWED_COINS,
            )

    def test_negative_volume_rejected(self) -> None:
        with pytest.raises(ValueError, match="non-negative"):
            parse_rest_candle(
                _candle({"v": "-1.0"}),
                allowed_coins=ALLOWED_COINS,
            )

    def test_missing_field_rejected(self) -> None:
        candle = _candle()
        del candle["o"]
        with pytest.raises(ValueError):
            parse_rest_candle(candle, allowed_coins=ALLOWED_COINS)

    def test_as_row_length_and_order(self) -> None:
        record = parse_rest_candle(_candle(), allowed_coins=ALLOWED_COINS)
        row = record.as_row()
        assert len(row) == 13
        assert row[0] == record.time  # time
        assert row[1] == SOURCE_ID  # source
        assert row[2] == "ETH"  # coin
        assert row[3] == "1h"  # interval

    def test_optional_trades_count_none(self) -> None:
        candle = _candle()
        del candle["n"]
        record = parse_rest_candle(candle, allowed_coins=ALLOWED_COINS)
        assert record.trades_count is None

    def test_payload_is_json_string(self) -> None:
        record = parse_rest_candle(_candle(), allowed_coins=ALLOWED_COINS)
        assert isinstance(record.payload, str)
        assert "ETH" in record.payload

    def test_open_candle_returns_none(self) -> None:
        """Open candles (T == t) return None — they can't be updated via DO NOTHING."""
        result = parse_rest_candle(
            _candle({"T": 1_700_000_000_000}),  # T == t → open
            allowed_coins=ALLOWED_COINS,
        )
        assert result is None
