DELETE FROM bronze_replay_registry
WHERE table_name = 'hl_l2book'
  AND source = 'hl_ws_l2book'
  AND btrim(dedup_key) = '';

CREATE OR REPLACE FUNCTION hl_l2book_register_replay_key() RETURNS trigger AS $$
BEGIN
    IF NEW.snapshot_hash IS NULL OR btrim(NEW.snapshot_hash) = '' THEN
        PERFORM bronze_raise_check_violation('hl_l2book_snapshot_hash_required', 'hl_l2book.snapshot_hash is required');
    END IF;
    IF NOT bronze_register_replay_key('hl_l2book', NEW.source, NEW.coin, '', NEW.snapshot_hash) THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

INSERT INTO bronze_replay_registry (table_name, source, scope_1, scope_2, dedup_key)
SELECT DISTINCT 'hl_l2book', source, coin, '', snapshot_hash
FROM hl_l2book
WHERE snapshot_hash IS NOT NULL
  AND btrim(snapshot_hash) <> ''
ON CONFLICT DO NOTHING;

CREATE OR REPLACE VIEW v_orderbook (time, source, exchange, coin, best_bid, best_ask, spread, imbalance) AS
WITH hyperliquid_ws_orderbook AS (
    SELECT DISTINCT ON (source, coin, snapshot_hash)
        time, source, 'hyperliquid'::TEXT, coin, best_bid_price, best_ask_price,
        COALESCE(spread, best_ask_price - best_bid_price),
        silver_orderbook_imbalance(best_bid_size, best_ask_size)
    FROM hl_l2book
    WHERE snapshot_hash IS NOT NULL
      AND btrim(snapshot_hash) <> ''
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
