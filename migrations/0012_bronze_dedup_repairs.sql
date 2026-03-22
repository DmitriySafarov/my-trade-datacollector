-- migrate: no-transaction
-- migrate: no-transaction
DO $$
DECLARE hypertable_name TEXT;
BEGIN
    FOREACH hypertable_name IN ARRAY ARRAY['hl_trades', 'hl_l2book', 'hl_asset_ctx', 'hl_candles', 'bn_agg_trades', 'bn_depth', 'bn_mark_price', 'bn_liquidations', 'bn_tickers', 'bn_klines'] LOOP
        EXECUTE format('SELECT remove_compression_policy(%L, if_exists => TRUE)', hypertable_name);
        EXECUTE format('SELECT decompress_chunk(chunk_info.chunk, if_compressed => TRUE) FROM show_chunks(%L) AS chunk_info(chunk)', hypertable_name);
        EXECUTE format('ALTER TABLE %I SET (timescaledb.compress = false)', hypertable_name);
    END LOOP;
END
$$;

CREATE TABLE IF NOT EXISTS hl_trade_registry (
    source TEXT NOT NULL,
    coin TEXT NOT NULL,
    tid BIGINT NOT NULL,
    PRIMARY KEY (source, coin, tid)
);
INSERT INTO hl_trade_registry (source, coin, tid)
SELECT DISTINCT source, coin, tid
FROM hl_trades
WHERE tid IS NOT NULL
ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION hl_trades_register_tid() RETURNS trigger AS $$
BEGIN
    IF NEW.tid IS NULL THEN
        RETURN NEW;
    END IF;
    INSERT INTO hl_trade_registry (source, coin, tid)
    VALUES (NEW.source, NEW.coin, NEW.tid)
    ON CONFLICT DO NOTHING;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS hl_trades_register_tid_before_insert ON hl_trades;
CREATE TRIGGER hl_trades_register_tid_before_insert
    BEFORE INSERT ON hl_trades
    FOR EACH ROW EXECUTE FUNCTION hl_trades_register_tid();
CREATE TABLE IF NOT EXISTS bn_agg_trade_registry (
    source TEXT NOT NULL,
    symbol TEXT NOT NULL,
    agg_trade_id BIGINT NOT NULL,
    PRIMARY KEY (source, symbol, agg_trade_id)
);
INSERT INTO bn_agg_trade_registry (source, symbol, agg_trade_id)
SELECT DISTINCT source, symbol, agg_trade_id
FROM bn_agg_trades
WHERE source IN ('bn_ws_agg_trades', 'bn_rest_agg_trades')
  AND agg_trade_id IS NOT NULL
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS bn_trade_registry (
    source TEXT NOT NULL,
    symbol TEXT NOT NULL,
    trade_id BIGINT NOT NULL,
    PRIMARY KEY (source, symbol, trade_id)
);
INSERT INTO bn_trade_registry (source, symbol, trade_id)
SELECT DISTINCT source, symbol, trade_id
FROM bn_agg_trades
WHERE source = 'bn_rest_trades'
  AND trade_id IS NOT NULL
ON CONFLICT DO NOTHING;

CREATE OR REPLACE FUNCTION bn_agg_trades_register_trade_id() RETURNS trigger AS $$
BEGIN
    IF NEW.source IN ('bn_ws_agg_trades', 'bn_rest_agg_trades') THEN
        IF NEW.agg_trade_id IS NULL THEN
            RETURN NEW;
        END IF;
        INSERT INTO bn_agg_trade_registry (source, symbol, agg_trade_id)
        VALUES (NEW.source, NEW.symbol, NEW.agg_trade_id)
        ON CONFLICT DO NOTHING;
        IF NOT FOUND THEN
            RETURN NULL;
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.source = 'bn_rest_trades' THEN
        IF NEW.trade_id IS NULL THEN
            RETURN NEW;
        END IF;
        INSERT INTO bn_trade_registry (source, symbol, trade_id)
        VALUES (NEW.source, NEW.symbol, NEW.trade_id)
        ON CONFLICT DO NOTHING;
        IF NOT FOUND THEN
            RETURN NULL;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS bn_agg_trades_register_trade_id_before_insert ON bn_agg_trades;
CREATE TRIGGER bn_agg_trades_register_trade_id_before_insert
    BEFORE INSERT ON bn_agg_trades
    FOR EACH ROW EXECUTE FUNCTION bn_agg_trades_register_trade_id();

CREATE TABLE IF NOT EXISTS news_dedup_registry (
    source TEXT NOT NULL,
    dedup_url TEXT NOT NULL,
    PRIMARY KEY (source, dedup_url)
);

INSERT INTO news_dedup_registry (source, dedup_url)
SELECT DISTINCT source, dedup_url
FROM news
ON CONFLICT DO NOTHING;

CREATE OR REPLACE FUNCTION news_register_dedup_url() RETURNS trigger AS $$
DECLARE
    registry_url TEXT;
BEGIN
    registry_url := COALESCE(NEW.canonical_url, NEW.url);
    INSERT INTO news_dedup_registry (source, dedup_url)
    VALUES (NEW.source, registry_url)
    ON CONFLICT DO NOTHING;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS news_register_dedup_url_before_insert ON news;
CREATE TRIGGER news_register_dedup_url_before_insert
    BEFORE INSERT ON news
    FOR EACH ROW EXECUTE FUNCTION news_register_dedup_url();

CREATE TABLE IF NOT EXISTS news_sentiment_dedup_registry (
    source TEXT NOT NULL,
    dedup_url TEXT NOT NULL,
    PRIMARY KEY (source, dedup_url)
);

INSERT INTO news_sentiment_dedup_registry (source, dedup_url)
SELECT DISTINCT source, dedup_url
FROM news_sentiment
ON CONFLICT DO NOTHING;

CREATE OR REPLACE FUNCTION news_sentiment_register_dedup_url() RETURNS trigger AS $$
DECLARE
    registry_url TEXT;
BEGIN
    registry_url := COALESCE(NEW.canonical_url, NEW.article_url);
    INSERT INTO news_sentiment_dedup_registry (source, dedup_url)
    VALUES (NEW.source, registry_url)
    ON CONFLICT DO NOTHING;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS news_sentiment_register_dedup_url_before_insert ON news_sentiment;
CREATE TRIGGER news_sentiment_register_dedup_url_before_insert
    BEFORE INSERT ON news_sentiment
    FOR EACH ROW EXECUTE FUNCTION news_sentiment_register_dedup_url();

SELECT remove_compression_policy('s3_l2book', if_exists => TRUE);
SELECT add_compression_policy('s3_l2book', compress_after => INTERVAL '45 days', if_not_exists => TRUE);
