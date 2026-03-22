CREATE TABLE IF NOT EXISTS bronze_replay_registry (
    table_name TEXT NOT NULL,
    source TEXT NOT NULL,
    scope_1 TEXT NOT NULL DEFAULT '',
    scope_2 TEXT NOT NULL DEFAULT '',
    dedup_key TEXT NOT NULL,
    PRIMARY KEY (table_name, source, scope_1, scope_2, dedup_key)
);

CREATE OR REPLACE FUNCTION bronze_register_replay_key(
    target_table TEXT,
    target_source TEXT,
    target_scope_1 TEXT,
    target_scope_2 TEXT,
    target_key TEXT
) RETURNS BOOLEAN AS $$
BEGIN
    INSERT INTO bronze_replay_registry (
        table_name,
        source,
        scope_1,
        scope_2,
        dedup_key
    )
    VALUES (
        target_table,
        target_source,
        target_scope_1,
        target_scope_2,
        target_key
    )
    ON CONFLICT DO NOTHING;
    RETURN FOUND;
END;
$$ LANGUAGE plpgsql;

INSERT INTO bronze_replay_registry (table_name, source, scope_1, scope_2, dedup_key)
SELECT 'hl_l2book', source, coin, '', snapshot_hash
FROM hl_l2book
WHERE snapshot_hash IS NOT NULL
ON CONFLICT DO NOTHING;
INSERT INTO bronze_replay_registry (table_name, source, scope_1, scope_2, dedup_key)
SELECT 'hl_asset_ctx', source, coin, '', event_hash
FROM hl_asset_ctx
WHERE event_hash IS NOT NULL
ON CONFLICT DO NOTHING;
INSERT INTO bronze_replay_registry (table_name, source, scope_1, scope_2, dedup_key)
SELECT 'hl_candles', source, coin, interval, event_hash
FROM hl_candles
WHERE event_hash IS NOT NULL
ON CONFLICT DO NOTHING;
INSERT INTO bronze_replay_registry (table_name, source, scope_1, scope_2, dedup_key)
SELECT 'bn_depth', source, symbol, '', COALESCE(final_update_id::text, last_update_id::text, snapshot_hash)
FROM bn_depth
WHERE COALESCE(final_update_id::text, last_update_id::text, snapshot_hash) IS NOT NULL
ON CONFLICT DO NOTHING;
INSERT INTO bronze_replay_registry (table_name, source, scope_1, scope_2, dedup_key)
SELECT 'bn_mark_price', source, symbol, '', event_hash
FROM bn_mark_price
WHERE event_hash IS NOT NULL
ON CONFLICT DO NOTHING;
INSERT INTO bronze_replay_registry (table_name, source, scope_1, scope_2, dedup_key)
SELECT 'bn_liquidations', source, symbol, '', event_hash
FROM bn_liquidations
WHERE event_hash IS NOT NULL
ON CONFLICT DO NOTHING;
INSERT INTO bronze_replay_registry (table_name, source, scope_1, scope_2, dedup_key)
SELECT 'bn_tickers', source, symbol, '', COALESCE(update_id::text, event_hash)
FROM bn_tickers
WHERE source IN ('bn_ws_ticker', 'bn_ws_mini_ticker', 'bn_ws_book_ticker')
  AND COALESCE(update_id::text, event_hash) IS NOT NULL
ON CONFLICT DO NOTHING;
INSERT INTO bronze_replay_registry (table_name, source, scope_1, scope_2, dedup_key)
SELECT 'bn_klines', source, symbol, interval, event_hash
FROM bn_klines
WHERE source = 'bn_ws_candles'
  AND event_hash IS NOT NULL
ON CONFLICT DO NOTHING;

CREATE OR REPLACE FUNCTION hl_l2book_register_replay_key() RETURNS trigger AS $$
BEGIN
    IF NEW.snapshot_hash IS NULL THEN
        RETURN NEW;
    END IF;
    IF NOT bronze_register_replay_key('hl_l2book', NEW.source, NEW.coin, '', NEW.snapshot_hash) THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS hl_l2book_register_replay_key_before_insert ON hl_l2book;
CREATE TRIGGER hl_l2book_register_replay_key_before_insert BEFORE INSERT ON hl_l2book FOR EACH ROW EXECUTE FUNCTION hl_l2book_register_replay_key();

CREATE OR REPLACE FUNCTION hl_asset_ctx_register_replay_key() RETURNS trigger AS $$
BEGIN
    IF NEW.event_hash IS NULL THEN
        RETURN NEW;
    END IF;
    IF NOT bronze_register_replay_key('hl_asset_ctx', NEW.source, NEW.coin, '', NEW.event_hash) THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS hl_asset_ctx_register_replay_key_before_insert ON hl_asset_ctx;
CREATE TRIGGER hl_asset_ctx_register_replay_key_before_insert BEFORE INSERT ON hl_asset_ctx FOR EACH ROW EXECUTE FUNCTION hl_asset_ctx_register_replay_key();

CREATE OR REPLACE FUNCTION hl_candles_register_replay_key() RETURNS trigger AS $$
BEGIN
    IF NEW.event_hash IS NULL THEN
        RETURN NEW;
    END IF;
    IF NOT bronze_register_replay_key('hl_candles', NEW.source, NEW.coin, NEW.interval, NEW.event_hash) THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS hl_candles_register_replay_key_before_insert ON hl_candles;
CREATE TRIGGER hl_candles_register_replay_key_before_insert BEFORE INSERT ON hl_candles FOR EACH ROW EXECUTE FUNCTION hl_candles_register_replay_key();

CREATE OR REPLACE FUNCTION bn_depth_register_replay_key() RETURNS trigger AS $$
DECLARE
    replay_key TEXT;
BEGIN
    replay_key := COALESCE(NEW.final_update_id::text, NEW.last_update_id::text, NEW.snapshot_hash);
    IF replay_key IS NULL THEN
        RETURN NEW;
    END IF;
    IF NOT bronze_register_replay_key('bn_depth', NEW.source, NEW.symbol, '', replay_key) THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS bn_depth_register_replay_key_before_insert ON bn_depth;
CREATE TRIGGER bn_depth_register_replay_key_before_insert BEFORE INSERT ON bn_depth FOR EACH ROW EXECUTE FUNCTION bn_depth_register_replay_key();

CREATE OR REPLACE FUNCTION bn_mark_price_register_replay_key() RETURNS trigger AS $$
BEGIN
    IF NEW.event_hash IS NULL THEN
        RETURN NEW;
    END IF;
    IF NOT bronze_register_replay_key('bn_mark_price', NEW.source, NEW.symbol, '', NEW.event_hash) THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS bn_mark_price_register_replay_key_before_insert ON bn_mark_price;
CREATE TRIGGER bn_mark_price_register_replay_key_before_insert BEFORE INSERT ON bn_mark_price FOR EACH ROW EXECUTE FUNCTION bn_mark_price_register_replay_key();

CREATE OR REPLACE FUNCTION bn_liquidations_register_replay_key() RETURNS trigger AS $$
BEGIN
    IF NEW.event_hash IS NULL THEN
        RETURN NEW;
    END IF;
    IF NOT bronze_register_replay_key('bn_liquidations', NEW.source, NEW.symbol, '', NEW.event_hash) THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS bn_liquidations_register_replay_key_before_insert ON bn_liquidations;
CREATE TRIGGER bn_liquidations_register_replay_key_before_insert BEFORE INSERT ON bn_liquidations FOR EACH ROW EXECUTE FUNCTION bn_liquidations_register_replay_key();

CREATE OR REPLACE FUNCTION bn_tickers_register_replay_key() RETURNS trigger AS $$
DECLARE
    replay_key TEXT;
BEGIN
    IF NEW.source NOT IN ('bn_ws_ticker', 'bn_ws_mini_ticker', 'bn_ws_book_ticker') THEN
        RETURN NEW;
    END IF;
    replay_key := COALESCE(NEW.update_id::text, NEW.event_hash);
    IF replay_key IS NULL THEN
        RETURN NEW;
    END IF;
    IF NOT bronze_register_replay_key('bn_tickers', NEW.source, NEW.symbol, '', replay_key) THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS bn_tickers_register_replay_key_before_insert ON bn_tickers;
CREATE TRIGGER bn_tickers_register_replay_key_before_insert BEFORE INSERT ON bn_tickers FOR EACH ROW EXECUTE FUNCTION bn_tickers_register_replay_key();

CREATE OR REPLACE FUNCTION bn_klines_register_replay_key() RETURNS trigger AS $$
BEGIN
    IF NEW.source <> 'bn_ws_candles' OR NEW.event_hash IS NULL THEN
        RETURN NEW;
    END IF;
    IF NOT bronze_register_replay_key('bn_klines', NEW.source, NEW.symbol, NEW.interval, NEW.event_hash) THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS bn_klines_register_replay_key_before_insert ON bn_klines;
CREATE TRIGGER bn_klines_register_replay_key_before_insert BEFORE INSERT ON bn_klines FOR EACH ROW EXECUTE FUNCTION bn_klines_register_replay_key();
