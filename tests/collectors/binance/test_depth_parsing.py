"""Unit tests for Binance depth20 @100ms parser."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, datetime

import pytest

from src.collectors.binance.depth_parsing import (
    SOURCE_ID,
    BinanceDepthRecord,
    parse_binance_depth,
)

VALID_PAYLOAD: dict[str, object] = {
    "lastUpdateId": 123456789,
    "E": 1711900800123,
    "T": 1711900800100,
    "bids": [["3020.50", "1.250"], ["3020.40", "2.500"]],
    "asks": [["3020.60", "1.100"], ["3020.70", "3.200"]],
}


class TestHappyPath:
    def test_parses_valid_payload(self) -> None:
        record = parse_binance_depth(VALID_PAYLOAD, symbol="ETHUSDT")
        assert isinstance(record, BinanceDepthRecord)
        assert record.source == SOURCE_ID
        assert record.symbol == "ETHUSDT"
        assert record.last_update_id == 123456789

    def test_best_bid_price(self) -> None:
        record = parse_binance_depth(VALID_PAYLOAD, symbol="ETHUSDT")
        assert record.best_bid_price == 3020.50

    def test_best_ask_price(self) -> None:
        record = parse_binance_depth(VALID_PAYLOAD, symbol="ETHUSDT")
        assert record.best_ask_price == 3020.60

    def test_time_from_event_time_field(self) -> None:
        record = parse_binance_depth(VALID_PAYLOAD, symbol="ETHUSDT")
        expected = datetime(2024, 3, 31, 16, 0, 0, 123_000, tzinfo=UTC)
        assert record.time == expected

    def test_bids_as_json_string(self) -> None:
        record = parse_binance_depth(VALID_PAYLOAD, symbol="ETHUSDT")
        bids = json.loads(record.bids)
        assert len(bids) == 2
        assert bids[0] == ["3020.50", "1.250"]

    def test_asks_as_json_string(self) -> None:
        record = parse_binance_depth(VALID_PAYLOAD, symbol="ETHUSDT")
        asks = json.loads(record.asks)
        assert len(asks) == 2
        assert asks[0] == ["3020.60", "1.100"]

    def test_payload_preserved_as_json(self) -> None:
        record = parse_binance_depth(VALID_PAYLOAD, symbol="ETHUSDT")
        payload = json.loads(record.payload)
        assert payload["lastUpdateId"] == 123456789
        assert payload["E"] == 1711900800123

    def test_snapshot_hash_is_sha256(self) -> None:
        record = parse_binance_depth(VALID_PAYLOAD, symbol="ETHUSDT")
        assert len(record.snapshot_hash) == 64
        assert all(c in "0123456789abcdef" for c in record.snapshot_hash)

    def test_snapshot_hash_deterministic(self) -> None:
        r1 = parse_binance_depth(VALID_PAYLOAD, symbol="ETHUSDT")
        r2 = parse_binance_depth(VALID_PAYLOAD, symbol="ETHUSDT")
        assert r1.snapshot_hash == r2.snapshot_hash

    def test_snapshot_hash_differs_for_different_data(self) -> None:
        r1 = parse_binance_depth(VALID_PAYLOAD, symbol="ETHUSDT")
        data2 = deepcopy(VALID_PAYLOAD)
        data2["lastUpdateId"] = 999999999
        r2 = parse_binance_depth(data2, symbol="ETHUSDT")
        assert r1.snapshot_hash != r2.snapshot_hash

    def test_copy_row_column_count(self) -> None:
        record = parse_binance_depth(VALID_PAYLOAD, symbol="ETHUSDT")
        row = record.as_copy_row()
        # 12 columns: time, source, symbol, first_update_id, final_update_id,
        # last_update_id, bids, asks, snapshot_hash,
        # best_bid_price, best_ask_price, payload
        assert len(row) == 12

    def test_copy_row_first_final_update_id_none(self) -> None:
        record = parse_binance_depth(VALID_PAYLOAD, symbol="ETHUSDT")
        row = record.as_copy_row()
        assert row[3] is None  # first_update_id
        assert row[4] is None  # final_update_id

    def test_btcusdt_symbol(self) -> None:
        record = parse_binance_depth(VALID_PAYLOAD, symbol="BTCUSDT")
        assert record.symbol == "BTCUSDT"

    def test_empty_bids_accepted(self) -> None:
        data = {**VALID_PAYLOAD, "bids": []}
        record = parse_binance_depth(data, symbol="ETHUSDT")
        assert record.best_bid_price is None
        assert json.loads(record.bids) == []

    def test_empty_asks_accepted(self) -> None:
        data = {**VALID_PAYLOAD, "asks": []}
        record = parse_binance_depth(data, symbol="ETHUSDT")
        assert record.best_ask_price is None
        assert json.loads(record.asks) == []

    def test_numeric_level_values(self) -> None:
        data = deepcopy(VALID_PAYLOAD)
        data["bids"] = [[3020.50, 1.250]]
        data["asks"] = [[3020.60, 1.100]]
        record = parse_binance_depth(data, symbol="ETHUSDT")
        assert record.best_bid_price == 3020.50
        assert record.best_ask_price == 3020.60

    def test_zero_price_accepted(self) -> None:
        data = deepcopy(VALID_PAYLOAD)
        data["bids"] = [["0", "1.0"]]
        record = parse_binance_depth(data, symbol="ETHUSDT")
        assert record.best_bid_price == 0.0

    def test_zero_qty_accepted(self) -> None:
        data = deepcopy(VALID_PAYLOAD)
        data["bids"] = [["100.0", "0"]]
        record = parse_binance_depth(data, symbol="ETHUSDT")
        assert record.best_bid_price == 100.0


class TestSymbolValidation:
    def test_unexpected_symbol_rejected(self) -> None:
        with pytest.raises(ValueError, match="Unexpected.*symbol"):
            parse_binance_depth(VALID_PAYLOAD, symbol="SOLUSDT")

    def test_lowercase_symbol_normalized(self) -> None:
        record = parse_binance_depth(VALID_PAYLOAD, symbol="ethusdt")
        assert record.symbol == "ETHUSDT"

    def test_custom_allowed_symbols(self) -> None:
        record = parse_binance_depth(
            VALID_PAYLOAD, symbol="SOLUSDT", allowed_symbols=("SOLUSDT",)
        )
        assert record.symbol == "SOLUSDT"


class TestMissingFields:
    @pytest.mark.parametrize("key", ["E", "lastUpdateId"])
    def test_missing_required_field_raises(self, key: str) -> None:
        data = deepcopy(VALID_PAYLOAD)
        del data[key]
        with pytest.raises(ValueError):
            parse_binance_depth(data, symbol="ETHUSDT")

    def test_missing_bids_raises(self) -> None:
        data = deepcopy(VALID_PAYLOAD)
        del data["bids"]
        with pytest.raises(ValueError, match="bids must be an array"):
            parse_binance_depth(data, symbol="ETHUSDT")

    def test_missing_asks_raises(self) -> None:
        data = deepcopy(VALID_PAYLOAD)
        del data["asks"]
        with pytest.raises(ValueError, match="asks must be an array"):
            parse_binance_depth(data, symbol="ETHUSDT")


class TestLevelValidation:
    def test_bids_not_array_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "bids": "invalid"}
        with pytest.raises(ValueError, match="bids must be an array"):
            parse_binance_depth(data, symbol="ETHUSDT")

    def test_asks_not_array_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "asks": 123}
        with pytest.raises(ValueError, match="asks must be an array"):
            parse_binance_depth(data, symbol="ETHUSDT")

    def test_level_not_pair_rejected(self) -> None:
        data = deepcopy(VALID_PAYLOAD)
        data["bids"] = [["3020.50"]]
        with pytest.raises(ValueError, match="price, qty.*pair"):
            parse_binance_depth(data, symbol="ETHUSDT")

    def test_level_three_elements_rejected(self) -> None:
        data = deepcopy(VALID_PAYLOAD)
        data["bids"] = [["3020.50", "1.0", "extra"]]
        with pytest.raises(ValueError, match="price, qty.*pair"):
            parse_binance_depth(data, symbol="ETHUSDT")

    def test_negative_price_rejected(self) -> None:
        data = deepcopy(VALID_PAYLOAD)
        data["bids"] = [["-1.0", "1.0"]]
        with pytest.raises(ValueError, match="non-negative"):
            parse_binance_depth(data, symbol="ETHUSDT")

    def test_negative_qty_rejected(self) -> None:
        data = deepcopy(VALID_PAYLOAD)
        data["asks"] = [["100.0", "-1.0"]]
        with pytest.raises(ValueError, match="non-negative"):
            parse_binance_depth(data, symbol="ETHUSDT")

    def test_nan_price_rejected(self) -> None:
        data = deepcopy(VALID_PAYLOAD)
        data["bids"] = [[float("nan"), "1.0"]]
        with pytest.raises(ValueError, match="finite"):
            parse_binance_depth(data, symbol="ETHUSDT")

    def test_inf_qty_rejected(self) -> None:
        data = deepcopy(VALID_PAYLOAD)
        data["asks"] = [["100.0", float("inf")]]
        with pytest.raises(ValueError, match="finite"):
            parse_binance_depth(data, symbol="ETHUSDT")

    def test_non_numeric_price_rejected(self) -> None:
        data = deepcopy(VALID_PAYLOAD)
        data["bids"] = [["abc", "1.0"]]
        with pytest.raises(ValueError, match="numeric"):
            parse_binance_depth(data, symbol="ETHUSDT")

    def test_boolean_qty_rejected(self) -> None:
        data = deepcopy(VALID_PAYLOAD)
        data["asks"] = [["100.0", True]]
        with pytest.raises(ValueError, match="numeric"):
            parse_binance_depth(data, symbol="ETHUSDT")

    def test_level_not_list_rejected(self) -> None:
        data = deepcopy(VALID_PAYLOAD)
        data["bids"] = ["invalid"]
        with pytest.raises(ValueError, match="price, qty.*pair"):
            parse_binance_depth(data, symbol="ETHUSDT")


class TestTimestampValidation:
    def test_zero_event_time_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "E": 0}
        with pytest.raises(ValueError, match="positive"):
            parse_binance_depth(data, symbol="ETHUSDT")

    def test_negative_event_time_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "E": -1}
        with pytest.raises(ValueError, match="positive"):
            parse_binance_depth(data, symbol="ETHUSDT")

    def test_overflow_event_time_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "E": 253_402_300_799_999 + 1}
        with pytest.raises(ValueError, match="out of range"):
            parse_binance_depth(data, symbol="ETHUSDT")

    def test_zero_last_update_id_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "lastUpdateId": 0}
        with pytest.raises(ValueError, match="positive"):
            parse_binance_depth(data, symbol="ETHUSDT")

    def test_negative_last_update_id_rejected(self) -> None:
        data = {**VALID_PAYLOAD, "lastUpdateId": -1}
        with pytest.raises(ValueError, match="positive"):
            parse_binance_depth(data, symbol="ETHUSDT")


class TestPayloadType:
    def test_non_mapping_rejected(self) -> None:
        with pytest.raises(ValueError, match="mapping"):
            parse_binance_depth([1, 2, 3], symbol="ETHUSDT")

    def test_none_rejected(self) -> None:
        with pytest.raises(ValueError, match="mapping"):
            parse_binance_depth(None, symbol="ETHUSDT")
