CREATE OR REPLACE VIEW silver_hyperliquid_asset_ctx_deduped AS
SELECT DISTINCT ON (source, coin, event_hash)
    time,
    source,
    coin,
    funding,
    open_interest,
    ingested_at
FROM hl_asset_ctx
WHERE event_hash IS NOT NULL
ORDER BY
    source,
    coin,
    event_hash,
    time,
    ingested_at;

CREATE OR REPLACE VIEW v_funding (time, source, exchange, coin, rate) AS
WITH hyperliquid_ws_funding AS (
    SELECT time, source, 'hyperliquid'::TEXT AS exchange, coin, funding AS rate
    FROM silver_hyperliquid_asset_ctx_deduped
    WHERE funding IS NOT NULL
),
hyperliquid_rest_funding AS (
    SELECT rest.time, rest.source, 'hyperliquid'::TEXT AS exchange, rest.coin, rest.funding AS rate
    FROM hl_rest_funding AS rest
    WHERE rest.funding IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM hyperliquid_ws_funding AS ws
          WHERE ws.coin = rest.coin
            AND ws.time = rest.time
      )
)
SELECT time, source, exchange, coin, rate
FROM hyperliquid_ws_funding
UNION ALL
SELECT time, source, exchange, coin, rate
FROM hyperliquid_rest_funding
UNION ALL
SELECT time, source, 'binance'::TEXT AS exchange, silver_base_asset(symbol), funding_rate
FROM bn_rest_funding
WHERE funding_rate IS NOT NULL;

CREATE OR REPLACE VIEW v_orderbook (time, source, exchange, coin, best_bid, best_ask, spread, imbalance) AS
WITH hyperliquid_ws_orderbook AS (
    SELECT DISTINCT ON (source, coin, snapshot_hash)
        time, source, 'hyperliquid'::TEXT, coin, best_bid_price, best_ask_price,
        COALESCE(spread, best_ask_price - best_bid_price),
        silver_orderbook_imbalance(best_bid_size, best_ask_size)
    FROM hl_l2book
    WHERE snapshot_hash IS NOT NULL
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

CREATE OR REPLACE VIEW v_oi (time, source, exchange, coin, open_interest) AS
SELECT time, source, 'hyperliquid'::TEXT AS exchange, coin, open_interest
FROM silver_hyperliquid_asset_ctx_deduped
WHERE open_interest IS NOT NULL
UNION ALL
SELECT time, source, 'binance'::TEXT AS exchange, silver_base_asset(symbol), open_interest
FROM bn_rest_oi
WHERE open_interest IS NOT NULL;

CREATE OR REPLACE VIEW v_candles (time, source, exchange, coin, interval, open, high, low, close, volume) AS
WITH hyperliquid_candles_raw AS (
    SELECT open_time, source, coin, interval, open, high, low, close, volume, hl_candles.time AS event_time, ingested_at, 0 AS source_rank
    FROM hl_candles
    WHERE is_closed
    UNION ALL
    SELECT open_time, source, coin, interval, open, high, low, close, volume, time AS event_time, ingested_at, 1 AS source_rank
    FROM hl_rest_candles
),
hyperliquid_candles AS (
    SELECT DISTINCT ON (coin, interval, open_time)
        open_time, source, 'hyperliquid'::TEXT, coin, interval, open, high, low, close, volume
    FROM hyperliquid_candles_raw
    ORDER BY coin, interval, open_time, source_rank, event_time DESC, ingested_at DESC
),
binance_candles AS (
    SELECT DISTINCT ON (open_time, silver_base_asset(symbol), interval)
        open_time, source, 'binance'::TEXT, silver_base_asset(symbol), interval, open, high, low, close, volume
    FROM bn_klines
    WHERE source IN ('bn_ws_candles', 'bn_rest_klines')
      AND is_closed
    ORDER BY open_time, silver_base_asset(symbol), interval, CASE source WHEN 'bn_ws_candles' THEN 0 ELSE 1 END, bn_klines.time DESC, ingested_at DESC
)
SELECT * FROM hyperliquid_candles
UNION ALL
SELECT * FROM binance_candles;
