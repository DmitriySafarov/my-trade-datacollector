TRUNCATE bronze_replay_registry;
INSERT INTO bronze_replay_registry (table_name, source, scope_1, scope_2, dedup_key)
SELECT DISTINCT 'hl_l2book', source, coin, '', snapshot_hash
FROM hl_l2book
WHERE snapshot_hash IS NOT NULL;
INSERT INTO bronze_replay_registry (table_name, source, scope_1, scope_2, dedup_key)
SELECT DISTINCT 'hl_asset_ctx', source, coin, '', event_hash
FROM hl_asset_ctx
WHERE event_hash IS NOT NULL;
INSERT INTO bronze_replay_registry (table_name, source, scope_1, scope_2, dedup_key)
SELECT DISTINCT 'hl_candles', source, coin, interval, event_hash
FROM hl_candles
WHERE event_hash IS NOT NULL;
INSERT INTO bronze_replay_registry (table_name, source, scope_1, scope_2, dedup_key)
SELECT DISTINCT
    'bn_depth',
    source,
    symbol,
    '',
    COALESCE(final_update_id::text, last_update_id::text, snapshot_hash)
FROM bn_depth
WHERE source = 'bn_ws_depth'
  AND COALESCE(final_update_id::text, last_update_id::text, snapshot_hash) IS NOT NULL;
INSERT INTO bronze_replay_registry (table_name, source, scope_1, scope_2, dedup_key)
SELECT DISTINCT 'bn_mark_price', source, symbol, '', event_hash
FROM bn_mark_price
WHERE event_hash IS NOT NULL;
INSERT INTO bronze_replay_registry (table_name, source, scope_1, scope_2, dedup_key)
SELECT DISTINCT 'bn_liquidations', source, symbol, '', event_hash
FROM bn_liquidations
WHERE event_hash IS NOT NULL;
INSERT INTO bronze_replay_registry (table_name, source, scope_1, scope_2, dedup_key)
SELECT DISTINCT 'bn_tickers', source, symbol, '', update_id::text
FROM bn_tickers
WHERE source = 'bn_ws_book_ticker'
  AND update_id IS NOT NULL;
INSERT INTO bronze_replay_registry (table_name, source, scope_1, scope_2, dedup_key)
SELECT DISTINCT 'bn_tickers', source, symbol, '', event_hash
FROM bn_tickers
WHERE source IN ('bn_ws_ticker', 'bn_ws_mini_ticker')
  AND event_hash IS NOT NULL;
INSERT INTO bronze_replay_registry (table_name, source, scope_1, scope_2, dedup_key)
SELECT DISTINCT 'bn_klines', source, symbol, interval, event_hash
FROM bn_klines
WHERE source = 'bn_ws_candles'
  AND event_hash IS NOT NULL;

CREATE OR REPLACE FUNCTION bn_tickers_register_replay_key() RETURNS trigger AS $$
DECLARE
    replay_key TEXT;
BEGIN
    IF NEW.source = 'bn_ws_book_ticker' THEN
        replay_key := NEW.update_id::text;
    ELSIF NEW.source IN ('bn_ws_ticker', 'bn_ws_mini_ticker') THEN
        replay_key := NEW.event_hash;
    ELSE
        RETURN NEW;
    END IF;
    IF replay_key IS NULL THEN
        RETURN NEW;
    END IF;
    IF NOT bronze_register_replay_key('bn_tickers', NEW.source, NEW.symbol, '', replay_key) THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
