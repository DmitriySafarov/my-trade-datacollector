CREATE VIEW v_trades (time, source, exchange, coin, side, price, size) AS
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

CREATE VIEW v_funding (time, source, exchange, coin, rate) AS
SELECT time, source, 'hyperliquid'::TEXT AS exchange, coin, funding AS rate
FROM hl_asset_ctx
WHERE funding IS NOT NULL
UNION ALL
SELECT time, source, 'binance'::TEXT AS exchange, silver_base_asset(symbol) AS coin, funding_rate AS rate
FROM bn_rest_funding
WHERE funding_rate IS NOT NULL;

CREATE VIEW v_orderbook (time, source, exchange, coin, best_bid, best_ask, spread, imbalance) AS
WITH hyperliquid_orderbook AS (
    SELECT DISTINCT ON (time, coin, COALESCE(snapshot_hash, format('%s|%s', best_bid_price, best_ask_price)))
        time,
        source,
        'hyperliquid'::TEXT AS exchange,
        coin,
        best_bid_price AS best_bid,
        best_ask_price AS best_ask,
        COALESCE(spread, best_ask_price - best_bid_price) AS spread,
        silver_orderbook_imbalance(best_bid_size, best_ask_size) AS imbalance
    FROM hl_l2book
    ORDER BY
        time,
        coin,
        COALESCE(snapshot_hash, format('%s|%s', best_bid_price, best_ask_price)),
        ingested_at
),
binance_orderbook AS (
    SELECT DISTINCT ON (time, silver_base_asset(symbol), COALESCE(final_update_id::text, last_update_id::text, snapshot_hash, format('%s|%s', best_bid_price, best_ask_price)))
        time,
        source,
        'binance'::TEXT AS exchange,
        silver_base_asset(symbol) AS coin,
        best_bid_price AS best_bid,
        best_ask_price AS best_ask,
        best_ask_price - best_bid_price AS spread,
        silver_orderbook_imbalance(silver_book_level_qty(bids), silver_book_level_qty(asks)) AS imbalance
    FROM bn_depth
    ORDER BY
        time,
        silver_base_asset(symbol),
        COALESCE(final_update_id::text, last_update_id::text, snapshot_hash, format('%s|%s', best_bid_price, best_ask_price)),
        CASE source WHEN 'bn_ws_depth' THEN 0 ELSE 1 END,
        ingested_at
)
SELECT time, source, exchange, coin, best_bid, best_ask, spread, imbalance FROM hyperliquid_orderbook
UNION ALL
SELECT time, source, exchange, coin, best_bid, best_ask, spread, imbalance FROM binance_orderbook;

CREATE VIEW v_oi (time, source, exchange, coin, open_interest) AS
SELECT time, source, 'hyperliquid'::TEXT AS exchange, coin, open_interest
FROM hl_asset_ctx
WHERE open_interest IS NOT NULL
UNION ALL
SELECT time, source, 'binance'::TEXT AS exchange, silver_base_asset(symbol) AS coin, open_interest
FROM bn_rest_oi
WHERE open_interest IS NOT NULL;

CREATE VIEW v_candles (time, source, exchange, coin, interval, open, high, low, close, volume) AS
WITH hyperliquid_candles AS (
    SELECT DISTINCT ON (open_time, coin, interval)
        open_time AS time,
        source,
        'hyperliquid'::TEXT AS exchange,
        coin,
        interval,
        open,
        high,
        low,
        close,
        volume
    FROM hl_candles
    WHERE is_closed
    ORDER BY open_time, coin, interval, hl_candles.time DESC, ingested_at DESC
),
binance_candles AS (
    SELECT DISTINCT ON (open_time, silver_base_asset(symbol), interval)
        open_time AS time,
        source,
        'binance'::TEXT AS exchange,
        silver_base_asset(symbol) AS coin,
        interval,
        open,
        high,
        low,
        close,
        volume
    FROM bn_klines
    WHERE source IN ('bn_ws_candles', 'bn_rest_klines')
      AND is_closed
    ORDER BY
        open_time,
        silver_base_asset(symbol),
        interval,
        CASE source WHEN 'bn_ws_candles' THEN 0 ELSE 1 END,
        bn_klines.time DESC,
        ingested_at DESC
)
SELECT time, source, exchange, coin, interval, open, high, low, close, volume FROM hyperliquid_candles
UNION ALL
SELECT time, source, exchange, coin, interval, open, high, low, close, volume FROM binance_candles;
