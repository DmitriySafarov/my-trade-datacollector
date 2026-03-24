"""Unit tests for Binance markPrice @1s parser."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, datetime

import pytest

from src.collectors.binance.mark_price_parsing import (
    SOURCE_ID,
    BinanceMarkPriceRecord,
    parse_binance_mark_price,
)

VALID_PAYLOAD: dict[str, object] = {
    "e": "markPriceUpdate",
    "E": 1711900800123,
    "s": "ETHUSDT",
    "p": "3021.50",
    "i": "3020.10",
    "P": "3019.80",
    "r": "0.00010000",
    "T": 1711929600000,
}


class TestHappyPath:
    def test_parses_valid_payload(self) -> None:
        record = parse_binance_mark_price(VALID_PAYLOAD)
        assert isinstance(record, BinanceMarkPriceRecord)
        assert record.source == SOURCE_ID
        assert record.symbol == "ETHUSDT"
        assert record.mark_price == 3021.50
        assert record.index_price == 3020.10
        assert record.estimated_settle_price == 3019.80
        assert record.funding_rate == 0.0001

    def test_premium_computed(self) -> None:
        record = parse_binance_mark_price(VALID_PAYLOAD)
        expected = 3021.50 - 3020.10
        assert abs(record.premium - expected) < 1e-10

    def test_time_from_event_time_field(self) -> None:
        record = parse_binance_mark_price(VALID_PAYLOAD)
        expected = datetime(2024, 3, 31, 16, 0, 0, 123_000, tzinfo=UTC)
        assert record.time == expected

    def test_next_funding_time(self) -> None:
        record = parse_binance_mark_price(VALID_PAYLOAD)
        assert record.next_funding_time is not None
        assert record.next_funding_time.tzinfo is not None

    def test_payload_preserved_as_json(self) -> None:
        record = parse_binance_mark_price(VALID_PAYLOAD)
        payload = json.loads(record.payload)
        assert payload["E"] == 1711900800123
        assert payload["s"] == "ETHUSDT"

    def test_event_hash_is_sha256(self) -> None:
        record = parse_binance_mark_price(VALID_PAYLOAD)
        assert len(record.event_hash) == 64
        assert all(c in "0123456789abcdef" for c in record.event_hash)

    def test_event_hash_deterministic(self) -> None:
        r1 = parse_binance_mark_price(VALID_PAYLOAD)
        r2 = parse_binance_mark_price(VALID_PAYLOAD)
        assert r1.event_hash == r2.event_hash

    def test_event_hash_differs_for_different_data(self) -> None:
        r1 = parse_binance_mark_price(VALID_PAYLOAD)
        data2 = {**VALID_PAYLOAD, "p": "3022.00"}
        r2 = parse_binance_mark_price(data2)
        assert r1.event_hash != r2.event_hash

    def test_copy_row_column_count(self) -> None:
        record = parse_binance_mark_price(VALID_PAYLOAD)
        row = record.as_copy_row()
        # 11 columns: time, source, symbol, mark_price, index_price,
        # estimated_settle_price, premium, funding_rate,
        # next_funding_time, event_hash, payload
        assert len(row) == 11

    def test_btcusdt_symbol(self) -> None:
        data = {**VALID_PAYLOAD, "s": "BTCUSDT"}
        record = parse_binance_mark_price(data)
        assert record.symbol == "BTCUSDT"

    def test_string_prices(self) -> None:
        data = {**VALID_PAYLOAD, "p": "99999.99", "i": "99998.50"}
        record = parse_binance_mark_price(data)
        assert record.mark_price == 99999.99
        assert record.index_price == 99998.50

    def test_numeric_prices(self) -> None:
        data = {**VALID_PAYLOAD, "p": 100.5, "i": 100.0}
        record = parse_binance_mark_price(data)
        assert record.mark_price == 100.5
        assert record.index_price == 100.0

    def test_next_funding_time_zero_becomes_none(self) -> None:
        data = {**VALID_PAYLOAD, "T": 0}
        record = parse_binance_mark_price(data)
        assert record.next_funding_time is None

    def test_negative_funding_rate(self) -> None:
        data = {**VALID_PAYLOAD, "r": "-0.0005"}
        record = parse_binance_mark_price(data)
        assert record.funding_rate == -0.0005

    def test_estimated_settle_price_absent(self) -> None:
        data = deepcopy(VALID_PAYLOAD)
        del data["P"]
        record = parse_binance_mark_price(data)
        assert record.estimated_settle_price is None

    def test_estimated_settle_price_empty_string(self) -> None:
        data = {**VALID_PAYLOAD, "P": ""}
        record = parse_binance_mark_price(data)
        assert record.estimated_settle_price is None

    def test_zero_mark_price_accepted(self) -> None:
        """Mark price of zero is valid during unusual market conditions."""
        data = {**VALID_PAYLOAD, "p": "0", "i": "0"}
        record = parse_binance_mark_price(data)
        assert record.mark_price == 0.0
        assert record.index_price == 0.0


class TestSymbolValidation:
    def test_unexpected_symbol_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "s": "SOLUSDT"}
        with pytest.raises(ValueError, match="Unexpected.*symbol"):
            parse_binance_mark_price(data)

    def test_lowercase_symbol_normalized(self) -> None:
        data = {**VALID_PAYLOAD, "s": "ethusdt"}
        record = parse_binance_mark_price(data)
        assert record.symbol == "ETHUSDT"

    def test_custom_allowed_symbols(self) -> None:
        data = {**VALID_PAYLOAD, "s": "SOLUSDT"}
        record = parse_binance_mark_price(data, allowed_symbols=("SOLUSDT",))
        assert record.symbol == "SOLUSDT"


class TestMissingFields:
    @pytest.mark.parametrize("key", ["s", "E", "p", "i", "r", "T"])
    def test_missing_required_field_raises(self, key: str) -> None:
        data = deepcopy(VALID_PAYLOAD)
        del data[key]
        with pytest.raises(ValueError):
            parse_binance_mark_price(data)


class TestNumericValidation:
    def test_negative_mark_price_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "p": "-1.5"}
        with pytest.raises(ValueError, match="non-negative"):
            parse_binance_mark_price(data)

    def test_negative_index_price_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "i": "-0.01"}
        with pytest.raises(ValueError, match="non-negative"):
            parse_binance_mark_price(data)

    def test_nan_mark_price_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "p": float("nan")}
        with pytest.raises(ValueError, match="finite"):
            parse_binance_mark_price(data)

    def test_inf_index_price_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "i": float("inf")}
        with pytest.raises(ValueError, match="finite"):
            parse_binance_mark_price(data)

    def test_non_numeric_price_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "p": "abc"}
        with pytest.raises(ValueError, match="numeric"):
            parse_binance_mark_price(data)

    def test_non_numeric_funding_rate_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "r": "abc"}
        with pytest.raises(ValueError, match="numeric"):
            parse_binance_mark_price(data)

    def test_nan_funding_rate_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "r": float("nan")}
        with pytest.raises(ValueError, match="finite"):
            parse_binance_mark_price(data)

    def test_negative_estimated_settle_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "P": "-1.0"}
        with pytest.raises(ValueError, match="non-negative"):
            parse_binance_mark_price(data)

    def test_boolean_funding_rate_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "r": True}
        with pytest.raises(ValueError, match="numeric"):
            parse_binance_mark_price(data)


class TestTimestampValidation:
    def test_zero_event_time_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "E": 0}
        with pytest.raises(ValueError, match="positive"):
            parse_binance_mark_price(data)

    def test_negative_event_time_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "E": -1}
        with pytest.raises(ValueError, match="positive"):
            parse_binance_mark_price(data)

    def test_overflow_event_time_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "E": 253_402_300_799_999 + 1}
        with pytest.raises(ValueError, match="out of range"):
            parse_binance_mark_price(data)

    def test_negative_next_funding_time_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "T": -1}
        with pytest.raises(ValueError, match="non-negative"):
            parse_binance_mark_price(data)


class TestPayloadType:
    def test_non_mapping_rejected(self) -> None:
        with pytest.raises(ValueError, match="mapping"):
            parse_binance_mark_price([1, 2, 3])

    def test_none_rejected(self) -> None:
        with pytest.raises(ValueError, match="mapping"):
            parse_binance_mark_price(None)
