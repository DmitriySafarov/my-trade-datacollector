CREATE TABLE IF NOT EXISTS bn_rest_delivery_legacy_null_delivery_time AS
SELECT * FROM bn_rest_delivery WITH NO DATA;
ALTER TABLE bn_rest_delivery_legacy_null_delivery_time ADD COLUMN IF NOT EXISTS archive_id BIGSERIAL;
ALTER TABLE bn_rest_delivery_legacy_null_delivery_time ADD COLUMN IF NOT EXISTS source_chunk_oid OID;
ALTER TABLE bn_rest_delivery_legacy_null_delivery_time ADD COLUMN IF NOT EXISTS source_ctid TID;
ALTER TABLE bn_rest_delivery_legacy_null_delivery_time ADD COLUMN IF NOT EXISTS quarantined_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE bn_rest_delivery_legacy_null_delivery_time ADD COLUMN IF NOT EXISTS quarantine_reason TEXT NOT NULL DEFAULT 'delivery_time_missing';
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

DO $$
BEGIN
    IF to_regclass('bn_rest_delivery_legacy_nulls') IS NOT NULL THEN
        EXECUTE $sql$
            INSERT INTO bn_rest_delivery_legacy_null_delivery_time (
                time,
                source,
                pair,
                delivery_time,
                delivery_price,
                payload,
                ingested_at,
                quarantined_at,
                quarantine_reason,
                archive_key
            )
            SELECT
                legacy.time,
                legacy.source,
                legacy.pair,
                legacy.delivery_time,
                legacy.delivery_price,
                legacy.payload,
                legacy.ingested_at,
                COALESCE(legacy.quarantined_at, now()),
                COALESCE(legacy.reason, 'delivery_time_missing'),
                md5(
                    concat_ws(
                        '|',
                        legacy.time::text,
                        legacy.source,
                        legacy.pair,
                        COALESCE(legacy.delivery_time::text, '<null>'),
                        COALESCE(legacy.delivery_price::text, '<null>'),
                        COALESCE(legacy.payload::text, '<null>'),
                        COALESCE(legacy.ingested_at::text, '<null>')
                    )
                )
            FROM bn_rest_delivery_legacy_nulls AS legacy
        $sql$;
        EXECUTE 'DROP TABLE bn_rest_delivery_legacy_nulls';
    END IF;
END
$$;
