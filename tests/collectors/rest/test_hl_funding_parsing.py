"""Tests for REST fundingHistory parsing."""

from __future__ import annotations

import pytest

from src.collectors.rest.hl_funding_parsing import (
    SOURCE_ID,
    RestFundingRecord,
    parse_rest_funding,
)

ALLOWED_COINS = ("ETH", "BTC")


def _funding(overrides: dict | None = None) -> dict:
    base = {
        "time": 1_710_780_000_000,
        "funding": "0.0001",
        "premium": "0.00005",
    }
    if overrides:
        base.update(overrides)
    return base


class TestParseRestFunding:
    def test_valid_record_produces_record(self) -> None:
        record = parse_rest_funding(_funding(), coin="ETH", allowed_coins=ALLOWED_COINS)
        assert isinstance(record, RestFundingRecord)
        assert record.source == SOURCE_ID
        assert record.coin == "ETH"
        assert record.funding == 0.0001
        assert record.premium == 0.00005
        assert record.mark_price is None
        assert record.oracle_price is None

    def test_time_parsed_from_milliseconds(self) -> None:
        record = parse_rest_funding(_funding(), coin="ETH", allowed_coins=ALLOWED_COINS)
        assert record.time.year == 2024
        assert record.time.tzinfo is not None

    def test_source_is_hl_rest_funding(self) -> None:
        record = parse_rest_funding(_funding(), coin="ETH", allowed_coins=ALLOWED_COINS)
        assert record.source == "hl_rest_funding"

    def test_btc_coin_accepted(self) -> None:
        record = parse_rest_funding(_funding(), coin="BTC", allowed_coins=ALLOWED_COINS)
        assert record.coin == "BTC"

    def test_coin_uppercased(self) -> None:
        record = parse_rest_funding(_funding(), coin="eth", allowed_coins=ALLOWED_COINS)
        assert record.coin == "ETH"

    def test_unknown_coin_rejected(self) -> None:
        with pytest.raises(ValueError, match="Unexpected"):
            parse_rest_funding(_funding(), coin="SOL", allowed_coins=ALLOWED_COINS)

    def test_missing_time_rejected(self) -> None:
        data = _funding()
        del data["time"]
        with pytest.raises(ValueError, match="time"):
            parse_rest_funding(data, coin="ETH", allowed_coins=ALLOWED_COINS)

    def test_negative_time_rejected(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            parse_rest_funding(
                _funding({"time": -1}), coin="ETH", allowed_coins=ALLOWED_COINS
            )

    def test_zero_time_rejected(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            parse_rest_funding(
                _funding({"time": 0}), coin="ETH", allowed_coins=ALLOWED_COINS
            )

    def test_missing_funding_rejected(self) -> None:
        data = _funding()
        del data["funding"]
        with pytest.raises(ValueError, match="funding"):
            parse_rest_funding(data, coin="ETH", allowed_coins=ALLOWED_COINS)

    def test_missing_premium_rejected(self) -> None:
        data = _funding()
        del data["premium"]
        with pytest.raises(ValueError, match="premium"):
            parse_rest_funding(data, coin="ETH", allowed_coins=ALLOWED_COINS)

    def test_non_object_rejected(self) -> None:
        with pytest.raises(ValueError, match="object"):
            parse_rest_funding("not a dict", coin="ETH", allowed_coins=ALLOWED_COINS)

    def test_non_numeric_funding_rejected(self) -> None:
        with pytest.raises(ValueError, match="numeric"):
            parse_rest_funding(
                _funding({"funding": "abc"}),
                coin="ETH",
                allowed_coins=ALLOWED_COINS,
            )

    def test_infinite_funding_rejected(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            parse_rest_funding(
                _funding({"funding": "inf"}),
                coin="ETH",
                allowed_coins=ALLOWED_COINS,
            )

    def test_boolean_time_rejected(self) -> None:
        with pytest.raises(ValueError, match="numeric"):
            parse_rest_funding(
                _funding({"time": True}),
                coin="ETH",
                allowed_coins=ALLOWED_COINS,
            )

    def test_negative_funding_allowed(self) -> None:
        """Funding rates can be negative."""
        record = parse_rest_funding(
            _funding({"funding": "-0.0003"}),
            coin="ETH",
            allowed_coins=ALLOWED_COINS,
        )
        assert record.funding == -0.0003

    def test_zero_funding_allowed(self) -> None:
        record = parse_rest_funding(
            _funding({"funding": "0"}),
            coin="ETH",
            allowed_coins=ALLOWED_COINS,
        )
        assert record.funding == 0.0

    def test_as_row_length_and_order(self) -> None:
        record = parse_rest_funding(_funding(), coin="ETH", allowed_coins=ALLOWED_COINS)
        row = record.as_row()
        assert len(row) == 8
        assert row[0] == record.time  # time
        assert row[1] == SOURCE_ID  # source
        assert row[2] == "ETH"  # coin
        assert row[3] == 0.0001  # funding
        assert row[4] == 0.00005  # premium
        assert row[5] is None  # mark_price
        assert row[6] is None  # oracle_price
        assert isinstance(row[7], str)  # payload

    def test_payload_is_json_string(self) -> None:
        record = parse_rest_funding(_funding(), coin="ETH", allowed_coins=ALLOWED_COINS)
        assert isinstance(record.payload, str)
        assert "0.0001" in record.payload

    def test_numeric_values_as_numbers_accepted(self) -> None:
        """API may return numbers directly (not strings)."""
        record = parse_rest_funding(
            _funding({"funding": 0.0002, "premium": 0.0001}),
            coin="ETH",
            allowed_coins=ALLOWED_COINS,
        )
        assert record.funding == 0.0002
        assert record.premium == 0.0001
