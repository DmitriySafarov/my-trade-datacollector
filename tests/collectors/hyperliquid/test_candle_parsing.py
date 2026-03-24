from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from src.collectors.hyperliquid.candle_parsing import (
    VALID_INTERVALS,
    parse_hyperliquid_candle,
)


EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _valid_candle(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "s": "ETH",
        "i": "1m",
        "t": 1711234500000,
        "T": 1711234559999,
        "o": "2126.0",
        "h": "2130.0",
        "l": "2125.0",
        "c": "2127.5",
        "v": "1234.567",
        "n": 42,
    }
    base.update(overrides)
    return base


class TestHappyPath:
    def test_parses_valid_candle(self) -> None:
        record = parse_hyperliquid_candle(
            _valid_candle(),
            source="hl_ws_candles",
            allowed_coins=("ETH",),
        )
        assert record.source == "hl_ws_candles"
        assert record.coin == "ETH"
        assert record.interval == "1m"
        assert record.open_time == EPOCH + timedelta(milliseconds=1711234500000)
        assert record.close_time == EPOCH + timedelta(milliseconds=1711234559999)
        assert record.open == pytest.approx(2126.0)
        assert record.high == pytest.approx(2130.0)
        assert record.low == pytest.approx(2125.0)
        assert record.close == pytest.approx(2127.5)
        assert record.volume == pytest.approx(1234.567)
        assert record.trades_count == 42
        assert record.is_closed is True
        assert len(record.event_hash) == 64

    def test_open_candle_has_is_closed_false(self) -> None:
        record = parse_hyperliquid_candle(
            _valid_candle(T=1711234500000),
            source="hl_ws_candles",
            allowed_coins=("ETH",),
        )
        assert record.is_closed is False

    def test_all_valid_intervals_accepted(self) -> None:
        for interval in VALID_INTERVALS:
            record = parse_hyperliquid_candle(
                _valid_candle(i=interval),
                source="hl_ws_candles",
                allowed_coins=("ETH",),
            )
            assert record.interval == interval

    def test_zero_volume_accepted(self) -> None:
        record = parse_hyperliquid_candle(
            _valid_candle(v="0.0", o="2126.0", h="2126.0", l="2126.0", c="2126.0"),
            source="hl_ws_candles",
            allowed_coins=("ETH",),
        )
        assert record.volume == 0.0

    def test_coin_case_insensitive(self) -> None:
        record = parse_hyperliquid_candle(
            _valid_candle(s="eth"),
            source="hl_ws_candles",
            allowed_coins=("ETH",),
        )
        assert record.coin == "ETH"

    def test_none_trades_count(self) -> None:
        candle = _valid_candle()
        del candle["n"]
        record = parse_hyperliquid_candle(
            candle,
            source="hl_ws_candles",
            allowed_coins=("ETH",),
        )
        assert record.trades_count is None

    def test_as_copy_row_length(self) -> None:
        record = parse_hyperliquid_candle(
            _valid_candle(),
            source="hl_ws_candles",
            allowed_coins=("ETH",),
        )
        row = record.as_copy_row()
        assert len(row) == 15

    def test_event_hash_deterministic(self) -> None:
        candle = _valid_candle()
        r1 = parse_hyperliquid_candle(candle, source="s", allowed_coins=("ETH",))
        r2 = parse_hyperliquid_candle(candle, source="s", allowed_coins=("ETH",))
        assert r1.event_hash == r2.event_hash


class TestValidation:
    def test_rejects_non_mapping(self) -> None:
        with pytest.raises(ValueError, match="must be an object"):
            parse_hyperliquid_candle(
                "not_a_dict",
                source="hl_ws_candles",
                allowed_coins=("ETH",),
            )

    def test_rejects_wrong_coin(self) -> None:
        with pytest.raises(ValueError, match="Unexpected"):
            parse_hyperliquid_candle(
                _valid_candle(s="SOL"),
                source="hl_ws_candles",
                allowed_coins=("ETH",),
            )

    def test_rejects_invalid_interval(self) -> None:
        with pytest.raises(ValueError, match="Unsupported"):
            parse_hyperliquid_candle(
                _valid_candle(i="7m"),
                source="hl_ws_candles",
                allowed_coins=("ETH",),
            )

    def test_rejects_close_time_before_open_time(self) -> None:
        with pytest.raises(ValueError, match="close_time must be >= open_time"):
            parse_hyperliquid_candle(
                _valid_candle(t=1711234560000, T=1711234500000),
                source="hl_ws_candles",
                allowed_coins=("ETH",),
            )

    def test_rejects_high_less_than_low(self) -> None:
        with pytest.raises(ValueError, match="high must be >= low"):
            parse_hyperliquid_candle(
                _valid_candle(h="2124.0", l="2125.0"),
                source="hl_ws_candles",
                allowed_coins=("ETH",),
            )

    def test_rejects_open_outside_range(self) -> None:
        with pytest.raises(ValueError, match="open must be within"):
            parse_hyperliquid_candle(
                _valid_candle(o="2131.0"),
                source="hl_ws_candles",
                allowed_coins=("ETH",),
            )

    def test_rejects_close_outside_range(self) -> None:
        with pytest.raises(ValueError, match="close must be within"):
            parse_hyperliquid_candle(
                _valid_candle(c="2124.0"),
                source="hl_ws_candles",
                allowed_coins=("ETH",),
            )

    def test_rejects_negative_volume(self) -> None:
        with pytest.raises(ValueError, match="non-negative"):
            parse_hyperliquid_candle(
                _valid_candle(v="-1.0"),
                source="hl_ws_candles",
                allowed_coins=("ETH",),
            )

    def test_rejects_zero_price(self) -> None:
        with pytest.raises(ValueError, match="must be positive"):
            parse_hyperliquid_candle(
                _valid_candle(o="0.0", h="0.0", l="0.0", c="0.0"),
                source="hl_ws_candles",
                allowed_coins=("ETH",),
            )

    def test_rejects_nan_price(self) -> None:
        with pytest.raises(ValueError, match="must be finite"):
            parse_hyperliquid_candle(
                _valid_candle(o="nan"),
                source="hl_ws_candles",
                allowed_coins=("ETH",),
            )

    def test_rejects_inf_price(self) -> None:
        with pytest.raises(ValueError, match="must be finite"):
            parse_hyperliquid_candle(
                _valid_candle(h="inf"),
                source="hl_ws_candles",
                allowed_coins=("ETH",),
            )

    def test_rejects_negative_time(self) -> None:
        with pytest.raises(ValueError, match="must be positive"):
            parse_hyperliquid_candle(
                _valid_candle(t=-1),
                source="hl_ws_candles",
                allowed_coins=("ETH",),
            )

    def test_rejects_overflow_time(self) -> None:
        with pytest.raises(ValueError, match="valid UTC timestamp"):
            parse_hyperliquid_candle(
                _valid_candle(t=999_999_999_999_999),
                source="hl_ws_candles",
                allowed_coins=("ETH",),
            )

    def test_rejects_negative_trades_count(self) -> None:
        with pytest.raises(ValueError, match="non-negative"):
            parse_hyperliquid_candle(
                _valid_candle(n=-1),
                source="hl_ws_candles",
                allowed_coins=("ETH",),
            )

    def test_rejects_bool_trades_count(self) -> None:
        with pytest.raises(ValueError, match="must be an integer"):
            parse_hyperliquid_candle(
                _valid_candle(n=True),
                source="hl_ws_candles",
                allowed_coins=("ETH",),
            )

    def test_rejects_missing_symbol(self) -> None:
        candle = _valid_candle()
        del candle["s"]
        with pytest.raises(ValueError, match="non-empty string"):
            parse_hyperliquid_candle(
                candle,
                source="hl_ws_candles",
                allowed_coins=("ETH",),
            )
