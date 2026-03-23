CREATE OR REPLACE FUNCTION hl_trade_positive_values_are_valid(
    trade_price DOUBLE PRECISION,
    trade_size DOUBLE PRECISION
) RETURNS BOOLEAN AS $$
    SELECT
        trade_price IS NOT NULL
        AND trade_size IS NOT NULL
        AND trade_price::text NOT IN ('NaN', 'Infinity', '-Infinity')
        AND trade_price > 0
        AND trade_size::text NOT IN ('NaN', 'Infinity', '-Infinity')
        AND trade_size > 0;
$$ LANGUAGE SQL IMMUTABLE;

DELETE FROM hl_trade_registry AS registry
WHERE EXISTS (
    SELECT 1
    FROM hl_trades AS trades
    WHERE trades.source = registry.source
      AND trades.coin = registry.coin
      AND trades.tid = registry.tid
      AND NOT hl_trade_positive_values_are_valid(trades.price, trades.size)
)
AND NOT EXISTS (
    SELECT 1
    FROM hl_trades AS trades
    WHERE trades.source = registry.source
      AND trades.coin = registry.coin
      AND trades.tid = registry.tid
      AND hl_trade_positive_values_are_valid(trades.price, trades.size)
);

INSERT INTO hl_trade_registry (bucket, source, coin, tid)
SELECT DISTINCT
    hl_trade_registry_bucket(source, coin, tid),
    source,
    coin,
    tid
FROM hl_trades
WHERE tid IS NOT NULL
  AND tid > 0
  AND hl_trade_positive_values_are_valid(price, size)
ON CONFLICT DO NOTHING;

CREATE OR REPLACE VIEW v_trades (time, source, exchange, coin, side, price, size) AS
WITH hyperliquid_trades AS (
    SELECT DISTINCT ON (coin, COALESCE(tid::text, format('%s|%s|%s|%s|%s', time, price, size, side, hash)))
        time,
        source,
        'hyperliquid'::TEXT AS exchange,
        coin,
        CASE WHEN side = 'B' THEN 'buy' WHEN side = 'A' THEN 'sell' ELSE lower(side) END AS side,
        price,
        size
    FROM hl_trades
    WHERE hl_trade_positive_values_are_valid(price, size)
    ORDER BY
        coin,
        COALESCE(tid::text, format('%s|%s|%s|%s|%s', time, price, size, side, hash)),
        time,
        ingested_at
),
binance_agg_trades AS (
    SELECT DISTINCT ON (symbol, dedup_trade_id)
        time,
        source,
        'binance'::TEXT AS exchange,
        silver_base_asset(symbol) AS coin,
        CASE WHEN is_buyer_maker IS NULL THEN NULL WHEN is_buyer_maker THEN 'sell' ELSE 'buy' END AS side,
        price,
        qty AS size
    FROM (
        SELECT
            time,
            source,
            symbol,
            COALESCE(agg_trade_id, trade_id) AS dedup_trade_id,
            is_buyer_maker,
            price,
            qty,
            ingested_at
        FROM bn_agg_trades
        WHERE source IN ('bn_ws_agg_trades', 'bn_rest_agg_trades')
    ) AS binance_agg_trade_rows
    WHERE dedup_trade_id IS NOT NULL
    ORDER BY
        symbol,
        dedup_trade_id,
        CASE source WHEN 'bn_ws_agg_trades' THEN 0 ELSE 1 END,
        time,
        ingested_at
)
SELECT time, source, exchange, coin, side, price, size FROM hyperliquid_trades
UNION ALL
SELECT time, source, exchange, coin, side, price, size FROM binance_agg_trades;

CREATE OR REPLACE FUNCTION hl_trades_register_tid() RETURNS trigger AS $$
BEGIN
    IF NEW.time IS NULL OR NEW.time <= TIMESTAMPTZ '1970-01-01 00:00:00+00' THEN
        PERFORM bronze_raise_check_violation('hl_trades_time_positive', 'hl_trades.time must be positive');
    END IF;
    IF NEW.side IS NULL OR NEW.side NOT IN ('A', 'B') THEN
        PERFORM bronze_raise_check_violation('hl_trades_side_valid', 'hl_trades.side must be A or B');
    END IF;
    IF NEW.price IS NULL OR NEW.price::text IN ('NaN', 'Infinity', '-Infinity') OR NEW.price <= 0 THEN
        PERFORM bronze_raise_check_violation('hl_trades_price_positive', 'hl_trades.price must be positive and finite');
    END IF;
    IF NEW.size IS NULL OR NEW.size::text IN ('NaN', 'Infinity', '-Infinity') OR NEW.size <= 0 THEN
        PERFORM bronze_raise_check_violation('hl_trades_size_positive', 'hl_trades.size must be positive and finite');
    END IF;
    IF NEW.hash IS NULL OR NEW.hash = '' THEN
        PERFORM bronze_raise_check_violation('hl_trades_hash_required', 'hl_trades.hash is required');
    END IF;
    IF NEW.tid IS NULL OR NEW.tid <= 0 THEN
        PERFORM bronze_raise_check_violation('hl_trades_tid_required', 'hl_trades.tid must be positive');
    END IF;
    IF NOT hl_trade_users_are_valid(NEW.users) THEN
        PERFORM bronze_raise_check_violation('hl_trades_users_pair_required', 'hl_trades.users must be a pair of addresses');
    END IF;
    IF NEW.payload IS NULL OR jsonb_typeof(NEW.payload) <> 'object' THEN
        PERFORM bronze_raise_check_violation('hl_trades_payload_object_required', 'hl_trades.payload must be a JSON object');
    END IF;
    INSERT INTO hl_trade_registry (bucket, source, coin, tid)
    VALUES (
        hl_trade_registry_bucket(NEW.source, NEW.coin, NEW.tid),
        NEW.source,
        NEW.coin,
        NEW.tid
    )
    ON CONFLICT DO NOTHING;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
