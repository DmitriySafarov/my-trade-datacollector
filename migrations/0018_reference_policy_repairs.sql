-- migrate: no-transaction
SELECT remove_compression_policy('bn_rest_delivery', if_exists => TRUE);
DO $$
BEGIN
    PERFORM decompress_chunk(chunk_info.chunk, if_compressed => TRUE)
    FROM show_chunks('bn_rest_delivery') AS chunk_info(chunk);
END
$$;
ALTER TABLE bn_rest_delivery SET (timescaledb.compress = false);

CREATE TABLE IF NOT EXISTS bn_rest_delivery_legacy_null_delivery_time AS
SELECT * FROM bn_rest_delivery WITH NO DATA;
ALTER TABLE bn_rest_delivery_legacy_null_delivery_time ADD COLUMN IF NOT EXISTS archive_id BIGSERIAL;
ALTER TABLE bn_rest_delivery_legacy_null_delivery_time ADD COLUMN IF NOT EXISTS source_chunk_oid OID;
ALTER TABLE bn_rest_delivery_legacy_null_delivery_time ADD COLUMN IF NOT EXISTS source_ctid TID;
ALTER TABLE bn_rest_delivery_legacy_null_delivery_time ADD COLUMN IF NOT EXISTS quarantined_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE bn_rest_delivery_legacy_null_delivery_time ADD COLUMN IF NOT EXISTS quarantine_reason TEXT NOT NULL DEFAULT 'delivery_time is NULL';
ALTER TABLE bn_rest_delivery_legacy_null_delivery_time ADD COLUMN IF NOT EXISTS archive_key TEXT;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'bn_rest_delivery_legacy_null_delivery_time'::regclass
          AND conname = 'bn_rest_delivery_legacy_null_delivery_time_pkey'
    ) THEN
        ALTER TABLE bn_rest_delivery_legacy_null_delivery_time
            ADD CONSTRAINT bn_rest_delivery_legacy_null_delivery_time_pkey
            PRIMARY KEY (archive_id);
    END IF;
END
$$;
DROP INDEX IF EXISTS bn_rest_delivery_legacy_null_delivery_time_uidx;
CREATE UNIQUE INDEX IF NOT EXISTS bn_rest_delivery_legacy_null_delivery_time_source_row_uidx
    ON bn_rest_delivery_legacy_null_delivery_time (source_chunk_oid, source_ctid)
    WHERE source_chunk_oid IS NOT NULL AND source_ctid IS NOT NULL;
CREATE INDEX IF NOT EXISTS bn_rest_delivery_legacy_null_delivery_time_archive_key_idx
    ON bn_rest_delivery_legacy_null_delivery_time (archive_key);
UPDATE bn_rest_delivery_legacy_null_delivery_time
SET archive_key = md5(
    concat_ws(
        '|',
        time::text,
        source,
        pair,
        COALESCE(delivery_time::text, '<null>'),
        COALESCE(delivery_price::text, '<null>'),
        COALESCE(payload::text, '<null>'),
        COALESCE(ingested_at::text, '<null>')
    )
)
WHERE archive_key IS NULL;
WITH null_rows AS (
    SELECT tableoid, ctid
    FROM bn_rest_delivery
    WHERE delivery_time IS NULL
)
INSERT INTO bn_rest_delivery_legacy_null_delivery_time (
    time,
    source,
    pair,
    delivery_time,
    delivery_price,
    payload,
    ingested_at,
    source_chunk_oid,
    source_ctid,
    quarantined_at,
    quarantine_reason,
    archive_key
)
SELECT
    delivery.time,
    delivery.source,
    delivery.pair,
    delivery.delivery_time,
    delivery.delivery_price,
    delivery.payload,
    delivery.ingested_at,
    delivery.tableoid,
    delivery.ctid,
    now(),
    'delivery_time is NULL',
    md5(
        concat_ws(
            '|',
            delivery.time::text,
            delivery.source,
            delivery.pair,
            COALESCE(delivery.delivery_time::text, '<null>'),
            COALESCE(delivery.delivery_price::text, '<null>'),
            COALESCE(delivery.payload::text, '<null>'),
            COALESCE(delivery.ingested_at::text, '<null>')
        )
    )
FROM bn_rest_delivery AS delivery
JOIN null_rows
    ON delivery.tableoid = null_rows.tableoid
   AND delivery.ctid = null_rows.ctid
ON CONFLICT DO NOTHING;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'bn_rest_delivery'::regclass
          AND conname = 'bn_rest_delivery_delivery_time_required'
    ) THEN
        ALTER TABLE bn_rest_delivery
            ADD CONSTRAINT bn_rest_delivery_delivery_time_required
            CHECK (delivery_time IS NOT NULL) NOT VALID;
    END IF;
END
$$;
CREATE TABLE IF NOT EXISTS bn_rest_delivery_registry (
    source TEXT NOT NULL,
    pair TEXT NOT NULL,
    delivery_time TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source, pair, delivery_time)
);
INSERT INTO bn_rest_delivery_registry (source, pair, delivery_time)
SELECT DISTINCT source, pair, delivery_time
FROM bn_rest_delivery
WHERE delivery_time IS NOT NULL
ON CONFLICT DO NOTHING;
CREATE INDEX IF NOT EXISTS bn_rest_delivery_source_pair_delivery_idx
    ON bn_rest_delivery (source, pair, delivery_time DESC);
CREATE OR REPLACE FUNCTION bn_rest_delivery_register_delivery_time() RETURNS trigger AS $$
DECLARE inserted_count INTEGER;
BEGIN
    IF NEW.delivery_time IS NULL THEN
        RETURN NEW;
    END IF;
    INSERT INTO bn_rest_delivery_registry (source, pair, delivery_time)
    VALUES (NEW.source, NEW.pair, NEW.delivery_time)
    ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    IF inserted_count = 0 THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS bn_rest_delivery_register_delivery_time_before_insert ON bn_rest_delivery;
CREATE TRIGGER bn_rest_delivery_register_delivery_time_before_insert
    BEFORE INSERT ON bn_rest_delivery
    FOR EACH ROW EXECUTE FUNCTION bn_rest_delivery_register_delivery_time();
DROP INDEX IF EXISTS bn_rest_delivery_source_time_pair_uidx;

ALTER TABLE bn_rest_delivery SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'source,pair',
    timescaledb.compress_orderby = 'delivery_time DESC,time DESC'
);
ALTER TABLE hl_trades SET (timescaledb.compress, timescaledb.compress_segmentby = 'source,coin', timescaledb.compress_orderby = 'time DESC');
ALTER TABLE hl_l2book SET (timescaledb.compress, timescaledb.compress_segmentby = 'source,coin', timescaledb.compress_orderby = 'time DESC');
ALTER TABLE hl_asset_ctx SET (timescaledb.compress, timescaledb.compress_segmentby = 'source,coin', timescaledb.compress_orderby = 'time DESC');
ALTER TABLE hl_candles SET (timescaledb.compress, timescaledb.compress_segmentby = 'source,coin,interval', timescaledb.compress_orderby = 'time DESC');
ALTER TABLE bn_agg_trades SET (timescaledb.compress, timescaledb.compress_segmentby = 'source,symbol', timescaledb.compress_orderby = 'time DESC');
ALTER TABLE bn_depth SET (timescaledb.compress, timescaledb.compress_segmentby = 'source,symbol', timescaledb.compress_orderby = 'time DESC');
ALTER TABLE bn_mark_price SET (timescaledb.compress, timescaledb.compress_segmentby = 'source,symbol', timescaledb.compress_orderby = 'time DESC');
ALTER TABLE bn_liquidations SET (timescaledb.compress, timescaledb.compress_segmentby = 'source,symbol', timescaledb.compress_orderby = 'time DESC,event_hash');
ALTER TABLE bn_tickers SET (timescaledb.compress, timescaledb.compress_segmentby = 'source,symbol', timescaledb.compress_orderby = 'time DESC,update_id,event_hash');
ALTER TABLE bn_klines SET (timescaledb.compress, timescaledb.compress_segmentby = 'source,symbol,interval,contract_type,pair', timescaledb.compress_orderby = 'time DESC,event_hash');
ALTER TABLE news SET (timescaledb.compress, timescaledb.compress_segmentby = 'source', timescaledb.compress_orderby = 'time DESC,dedup_url');
ALTER TABLE news_sentiment SET (timescaledb.compress, timescaledb.compress_segmentby = 'source', timescaledb.compress_orderby = 'time DESC,dedup_url');
DO $$
DECLARE hypertable_name TEXT;
BEGIN
    FOREACH hypertable_name IN ARRAY ARRAY[
        'hl_trades', 'hl_l2book', 'hl_asset_ctx', 'hl_candles',
        'bn_agg_trades', 'bn_depth', 'bn_mark_price', 'bn_liquidations',
        'bn_tickers', 'bn_klines'
    ] LOOP
        EXECUTE format(
            'SELECT add_compression_policy(%L, compress_after => INTERVAL ''48 hours'', if_not_exists => TRUE)',
            hypertable_name
        );
    END LOOP;
    FOREACH hypertable_name IN ARRAY ARRAY[
        'hl_rest_candles', 'hl_rest_funding', 'bn_rest_funding', 'bn_rest_oi',
        'bn_rest_oi_hist', 'bn_rest_ls_top_pos', 'bn_rest_ls_top_acc',
        'bn_rest_ls_global', 'bn_rest_taker_vol', 'bn_rest_premium',
        'bn_rest_basis', 'bn_rest_funding_info', 'bn_rest_delivery',
        'bn_rest_exchange_info', 'bn_rest_constituents', 'news',
        'news_sentiment', 'fred_data', 'fear_greed', 'cc_exchange_vol',
        'cc_price', 'finnhub_quotes', 'market_status'
    ] LOOP
        EXECUTE format('SELECT remove_compression_policy(%L, if_exists => TRUE)', hypertable_name);
        EXECUTE format(
            'SELECT add_compression_policy(%L, compress_after => INTERVAL ''8 days'', if_not_exists => TRUE)',
            hypertable_name
        );
    END LOOP;
END
$$;
SELECT remove_compression_policy('s3_l2book', if_exists => TRUE);
SELECT add_compression_policy('s3_l2book', compress_after => INTERVAL '45 days', if_not_exists => TRUE);
