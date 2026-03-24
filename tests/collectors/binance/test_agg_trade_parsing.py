"""Unit tests for Binance aggTrade parser."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, datetime

import pytest

from src.collectors.binance.agg_trade_parsing import (
    SOURCE_ID,
    BinanceAggTradeRecord,
    parse_binance_agg_trade,
)

VALID_PAYLOAD: dict[str, object] = {
    "e": "aggTrade",
    "E": 1711900800123,
    "s": "ETHUSDT",
    "a": 456789012,
    "p": "3021.50",
    "q": "1.250",
    "f": 900000001,
    "l": 900000003,
    "T": 1711900800100,
    "m": False,
}


class TestHappyPath:
    def test_parses_valid_payload(self) -> None:
        record = parse_binance_agg_trade(VALID_PAYLOAD)
        assert isinstance(record, BinanceAggTradeRecord)
        assert record.source == SOURCE_ID
        assert record.symbol == "ETHUSDT"
        assert record.agg_trade_id == 456789012
        assert record.first_trade_id == 900000001
        assert record.last_trade_id == 900000003
        assert record.price == 3021.50
        assert record.qty == 1.250
        assert record.is_buyer_maker is False

    def test_time_from_trade_time_field(self) -> None:
        record = parse_binance_agg_trade(VALID_PAYLOAD)
        expected = datetime(2024, 3, 31, 16, 0, 0, 100_000, tzinfo=UTC)
        assert record.time == expected

    def test_payload_preserved_as_json(self) -> None:
        record = parse_binance_agg_trade(VALID_PAYLOAD)
        payload = json.loads(record.payload)
        assert payload["a"] == 456789012
        assert payload["E"] == 1711900800123

    def test_copy_row_column_count(self) -> None:
        record = parse_binance_agg_trade(VALID_PAYLOAD)
        row = record.as_copy_row()
        # 12 columns: time, source, symbol, trade_id(None), agg_trade_id,
        # first_trade_id, last_trade_id, price, qty, is_buyer_maker,
        # is_best_match(None), payload
        assert len(row) == 12
        assert row[3] is None  # trade_id
        assert row[10] is None  # is_best_match

    def test_buyer_maker_true(self) -> None:
        data = {**VALID_PAYLOAD, "m": True}
        record = parse_binance_agg_trade(data)
        assert record.is_buyer_maker is True

    def test_btcusdt_symbol(self) -> None:
        data = {**VALID_PAYLOAD, "s": "BTCUSDT"}
        record = parse_binance_agg_trade(data)
        assert record.symbol == "BTCUSDT"

    def test_string_price_and_qty(self) -> None:
        data = {**VALID_PAYLOAD, "p": "99999.99", "q": "0.001"}
        record = parse_binance_agg_trade(data)
        assert record.price == 99999.99
        assert record.qty == 0.001

    def test_numeric_price_and_qty(self) -> None:
        data = {**VALID_PAYLOAD, "p": 100.5, "q": 2.0}
        record = parse_binance_agg_trade(data)
        assert record.price == 100.5
        assert record.qty == 2.0

    def test_first_trade_id_zero_accepted(self) -> None:
        data = {**VALID_PAYLOAD, "f": 0}
        record = parse_binance_agg_trade(data)
        assert record.first_trade_id == 0

    def test_last_trade_id_zero_accepted(self) -> None:
        data = {**VALID_PAYLOAD, "l": 0}
        record = parse_binance_agg_trade(data)
        assert record.last_trade_id == 0


class TestSymbolValidation:
    def test_unexpected_symbol_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "s": "SOLUSDT"}
        with pytest.raises(ValueError, match="Unexpected.*symbol"):
            parse_binance_agg_trade(data)

    def test_lowercase_symbol_normalized(self) -> None:
        data = {**VALID_PAYLOAD, "s": "ethusdt"}
        record = parse_binance_agg_trade(data)
        assert record.symbol == "ETHUSDT"

    def test_custom_allowed_symbols(self) -> None:
        data = {**VALID_PAYLOAD, "s": "SOLUSDT"}
        record = parse_binance_agg_trade(data, allowed_symbols=("SOLUSDT",))
        assert record.symbol == "SOLUSDT"


class TestMissingFields:
    @pytest.mark.parametrize("key", ["s", "a", "p", "q", "f", "l", "T", "m"])
    def test_missing_required_field_raises(self, key: str) -> None:
        data = deepcopy(VALID_PAYLOAD)
        del data[key]
        with pytest.raises(ValueError):
            parse_binance_agg_trade(data)


class TestNumericValidation:
    def test_zero_price_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "p": "0"}
        with pytest.raises(ValueError, match="positive"):
            parse_binance_agg_trade(data)

    def test_negative_price_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "p": "-1.5"}
        with pytest.raises(ValueError, match="positive"):
            parse_binance_agg_trade(data)

    def test_zero_qty_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "q": "0"}
        with pytest.raises(ValueError, match="positive"):
            parse_binance_agg_trade(data)

    def test_nan_price_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "p": float("nan")}
        with pytest.raises(ValueError, match="finite"):
            parse_binance_agg_trade(data)

    def test_inf_qty_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "q": float("inf")}
        with pytest.raises(ValueError, match="finite"):
            parse_binance_agg_trade(data)

    def test_non_numeric_price_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "p": "abc"}
        with pytest.raises(ValueError, match="numeric"):
            parse_binance_agg_trade(data)

    def test_boolean_agg_trade_id_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "a": True}
        with pytest.raises(ValueError, match="integer"):
            parse_binance_agg_trade(data)

    def test_negative_agg_trade_id_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "a": -1}
        with pytest.raises(ValueError, match="positive"):
            parse_binance_agg_trade(data)

    def test_negative_first_trade_id_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "f": -1}
        with pytest.raises(ValueError, match="non-negative"):
            parse_binance_agg_trade(data)

    def test_bigint_overflow_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "a": 9_223_372_036_854_775_808}
        with pytest.raises(ValueError, match="out of range"):
            parse_binance_agg_trade(data)


class TestTimestampValidation:
    def test_zero_trade_time_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "T": 0}
        with pytest.raises(ValueError, match="positive"):
            parse_binance_agg_trade(data)

    def test_negative_trade_time_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "T": -1}
        with pytest.raises(ValueError, match="positive"):
            parse_binance_agg_trade(data)

    def test_overflow_trade_time_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "T": 253_402_300_799_999 + 1}
        with pytest.raises(ValueError, match="out of range"):
            parse_binance_agg_trade(data)


class TestBooleanValidation:
    def test_non_boolean_m_field_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "m": 1}
        with pytest.raises(ValueError, match="boolean"):
            parse_binance_agg_trade(data)

    def test_string_m_field_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "m": "true"}
        with pytest.raises(ValueError, match="boolean"):
            parse_binance_agg_trade(data)


class TestPayloadType:
    def test_non_mapping_rejected(self) -> None:
        with pytest.raises(ValueError, match="mapping"):
            parse_binance_agg_trade([1, 2, 3])

    def test_none_rejected(self) -> None:
        with pytest.raises(ValueError, match="mapping"):
            parse_binance_agg_trade(None)
