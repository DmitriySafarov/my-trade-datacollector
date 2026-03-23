CREATE OR REPLACE FUNCTION hl_l2book_value_is_finite(
    value DOUBLE PRECISION
) RETURNS BOOLEAN AS $$
    SELECT value IS NULL OR value::text NOT IN ('NaN', 'Infinity', '-Infinity');
$$ LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION hl_l2book_value_is_positive_and_finite(
    value DOUBLE PRECISION
) RETURNS BOOLEAN AS $$
    SELECT value IS NULL OR (hl_l2book_value_is_finite(value) AND value > 0);
$$ LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION hl_l2book_value_is_non_negative_and_finite(
    value DOUBLE PRECISION
) RETURNS BOOLEAN AS $$
    SELECT value IS NULL OR (hl_l2book_value_is_finite(value) AND value >= 0);
$$ LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION hl_l2book_derived_values_are_finite(
    best_bid_price DOUBLE PRECISION,
    best_bid_size DOUBLE PRECISION,
    best_ask_price DOUBLE PRECISION,
    best_ask_size DOUBLE PRECISION,
    spread DOUBLE PRECISION,
    bid_notional DOUBLE PRECISION,
    ask_notional DOUBLE PRECISION
) RETURNS BOOLEAN AS $$
    SELECT
        hl_l2book_value_is_positive_and_finite(best_bid_price)
        AND hl_l2book_value_is_non_negative_and_finite(best_bid_size)
        AND hl_l2book_value_is_positive_and_finite(best_ask_price)
        AND hl_l2book_value_is_non_negative_and_finite(best_ask_size)
        AND hl_l2book_value_is_finite(spread)
        AND hl_l2book_value_is_non_negative_and_finite(bid_notional)
        AND hl_l2book_value_is_non_negative_and_finite(ask_notional);
$$ LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION hl_l2book_register_replay_key() RETURNS trigger AS $$
BEGIN
    IF NEW.snapshot_hash IS NULL OR btrim(NEW.snapshot_hash) = '' THEN
        PERFORM bronze_raise_check_violation('hl_l2book_snapshot_hash_required', 'hl_l2book.snapshot_hash is required');
    END IF;
    IF NOT hl_l2book_derived_values_are_finite(
        NEW.best_bid_price,
        NEW.best_bid_size,
        NEW.best_ask_price,
        NEW.best_ask_size,
        NEW.spread,
        NEW.bid_notional,
        NEW.ask_notional
    ) THEN
        PERFORM bronze_raise_check_violation('hl_l2book_derived_values_finite', 'hl_l2book derived values must be finite');
    END IF;
    IF NOT bronze_register_replay_key('hl_l2book', NEW.source, NEW.coin, '', NEW.snapshot_hash) THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE VIEW v_orderbook (time, source, exchange, coin, best_bid, best_ask, spread, imbalance) AS
WITH hyperliquid_ws_orderbook AS (
    SELECT DISTINCT ON (source, coin, snapshot_hash)
        time, source, 'hyperliquid'::TEXT, coin, best_bid_price, best_ask_price,
        COALESCE(spread, best_ask_price - best_bid_price),
        silver_orderbook_imbalance(best_bid_size, best_ask_size)
    FROM hl_l2book
    WHERE snapshot_hash IS NOT NULL
      AND btrim(snapshot_hash) <> ''
      AND hl_l2book_value_is_positive_and_finite(best_bid_price)
      AND hl_l2book_value_is_non_negative_and_finite(best_bid_size)
      AND hl_l2book_value_is_positive_and_finite(best_ask_price)
      AND hl_l2book_value_is_non_negative_and_finite(best_ask_size)
      AND hl_l2book_value_is_finite(spread)
    ORDER BY source, coin, snapshot_hash, time, ingested_at
),
binance_ws_orderbook AS (
    SELECT DISTINCT ON (symbol, COALESCE(final_update_id::text, last_update_id::text, snapshot_hash))
        time, source, 'binance'::TEXT, silver_base_asset(symbol), best_bid_price, best_ask_price,
        best_ask_price - best_bid_price,
        silver_orderbook_imbalance(silver_book_level_qty(bids), silver_book_level_qty(asks))
    FROM bn_depth
    WHERE source = 'bn_ws_depth'
      AND COALESCE(final_update_id::text, last_update_id::text, snapshot_hash) IS NOT NULL
    ORDER BY symbol, COALESCE(final_update_id::text, last_update_id::text, snapshot_hash), time, ingested_at
),
binance_rest_orderbook AS (
    SELECT time, source, 'binance'::TEXT, silver_base_asset(symbol), best_bid_price, best_ask_price,
        best_ask_price - best_bid_price,
        silver_orderbook_imbalance(silver_book_level_qty(bids), silver_book_level_qty(asks))
    FROM bn_depth
    WHERE source = 'bn_rest_depth'
)
SELECT * FROM hyperliquid_ws_orderbook
UNION ALL
SELECT * FROM binance_ws_orderbook
UNION ALL
SELECT * FROM binance_rest_orderbook;
