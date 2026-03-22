-- migrate: no-transaction
SELECT remove_compression_policy('bn_rest_delivery', if_exists => TRUE);
DO $$
BEGIN
    PERFORM decompress_chunk(chunk_info.chunk, if_compressed => TRUE)
    FROM show_chunks('bn_rest_delivery') AS chunk_info(chunk);
END
$$;
ALTER TABLE bn_rest_delivery SET (timescaledb.compress = false);

ALTER TABLE bn_rest_delivery DROP CONSTRAINT IF EXISTS bn_rest_delivery_delivery_time_required;
DROP TRIGGER IF EXISTS bn_rest_delivery_register_delivery_time_before_insert ON bn_rest_delivery;
ALTER TABLE bn_rest_delivery_legacy_null_delivery_time ADD COLUMN IF NOT EXISTS archive_key TEXT;
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
INSERT INTO bn_rest_delivery (
    time,
    source,
    pair,
    delivery_time,
    delivery_price,
    payload,
    ingested_at
)
SELECT
    legacy.time,
    legacy.source,
    legacy.pair,
    legacy.delivery_time,
    legacy.delivery_price,
    legacy.payload,
    legacy.ingested_at
FROM (
    SELECT DISTINCT
        time,
        source,
        pair,
        delivery_time,
        delivery_price,
        payload,
        ingested_at
    FROM bn_rest_delivery_legacy_null_delivery_time
    WHERE delivery_time IS NULL
) AS legacy
WHERE legacy.delivery_time IS NULL
  AND NOT EXISTS (
    SELECT 1
    FROM bn_rest_delivery AS live
    WHERE live.source = legacy.source
      AND live.time = legacy.time
      AND live.pair = legacy.pair
      AND live.delivery_time IS NOT DISTINCT FROM legacy.delivery_time
      AND live.delivery_price IS NOT DISTINCT FROM legacy.delivery_price
      AND live.payload IS NOT DISTINCT FROM legacy.payload
      AND live.ingested_at IS NOT DISTINCT FROM legacy.ingested_at
  );

CREATE TABLE IF NOT EXISTS bn_rest_delivery_registry (
    source TEXT NOT NULL,
    pair TEXT NOT NULL,
    delivery_time TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source, pair, delivery_time)
);
TRUNCATE bn_rest_delivery_registry;
INSERT INTO bn_rest_delivery_registry (source, pair, delivery_time)
SELECT DISTINCT source, pair, delivery_time
FROM bn_rest_delivery
WHERE delivery_time IS NOT NULL;
CREATE INDEX IF NOT EXISTS bn_rest_delivery_source_pair_delivery_idx
    ON bn_rest_delivery (source, pair, delivery_time DESC);
CREATE OR REPLACE FUNCTION bn_rest_delivery_register_delivery_time() RETURNS trigger AS $$
BEGIN
    IF NEW.delivery_time IS NULL THEN
        RAISE EXCEPTION 'bn_rest_delivery.delivery_time is required'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'bn_rest_delivery_delivery_time_required';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM bn_rest_delivery
        WHERE source = NEW.source
          AND time = NEW.time
          AND pair = NEW.pair
          AND delivery_time IS NOT DISTINCT FROM NEW.delivery_time
    ) THEN
        RETURN NULL;
    END IF;
    INSERT INTO bn_rest_delivery_registry (source, pair, delivery_time)
    VALUES (NEW.source, NEW.pair, NEW.delivery_time)
    ON CONFLICT DO NOTHING;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER bn_rest_delivery_register_delivery_time_before_insert
    BEFORE INSERT ON bn_rest_delivery
    FOR EACH ROW EXECUTE FUNCTION bn_rest_delivery_register_delivery_time();
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

ALTER TABLE bn_rest_delivery SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'source,pair',
    timescaledb.compress_orderby = 'delivery_time DESC,time DESC'
);
SELECT add_compression_policy(
    'bn_rest_delivery',
    compress_after => INTERVAL '8 days',
    if_not_exists => TRUE
);
