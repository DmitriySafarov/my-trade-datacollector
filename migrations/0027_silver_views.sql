CREATE OR REPLACE FUNCTION silver_base_asset(market_symbol TEXT)
RETURNS TEXT AS $$
    SELECT CASE
        WHEN market_symbol IS NULL THEN NULL
        ELSE regexp_replace(upper(btrim(market_symbol)), '(USDT|USDC|BUSD|FDUSD|USD)$', '')
    END
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION silver_book_level_qty(levels JSONB)
RETURNS DOUBLE PRECISION AS $$
    SELECT COALESCE(NULLIF(levels -> 0 ->> 1, '')::double precision, 0)
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION silver_orderbook_imbalance(
    bid_total DOUBLE PRECISION,
    ask_total DOUBLE PRECISION
)
RETURNS DOUBLE PRECISION AS $$
    SELECT
        (COALESCE(bid_total, 0) - COALESCE(ask_total, 0))
        / NULLIF(COALESCE(bid_total, 0) + COALESCE(ask_total, 0), 0)
$$ LANGUAGE sql IMMUTABLE;
