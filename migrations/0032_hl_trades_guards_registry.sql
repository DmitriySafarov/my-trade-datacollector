CREATE OR REPLACE FUNCTION hl_trade_registry_bucket(
    target_source TEXT,
    target_coin TEXT,
    target_tid BIGINT
) RETURNS SMALLINT AS $$
    SELECT mod(
        abs(hashtext(target_source || '|' || target_coin || '|' || target_tid::text)::bigint),
        32
    )::smallint;
$$ LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION hl_trade_users_are_valid(users_json JSONB) RETURNS BOOLEAN AS $$
    SELECT CASE
        WHEN users_json IS NULL
            OR jsonb_typeof(users_json) <> 'array'
            OR jsonb_array_length(users_json) <> 2 THEN FALSE
        ELSE NOT EXISTS (
            SELECT 1
            FROM jsonb_array_elements(users_json) AS element(value)
            WHERE jsonb_typeof(value) <> 'string' OR value #>> '{}' = ''
        )
    END;
$$ LANGUAGE SQL IMMUTABLE;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_class AS class
        JOIN pg_namespace AS namespace ON namespace.oid = class.relnamespace
        WHERE namespace.nspname = 'public'
          AND class.relname = 'hl_trade_registry'
          AND class.relkind = 'p'
    ) THEN
        RETURN;
    END IF;
    ALTER TABLE hl_trade_registry RENAME TO hl_trade_registry_legacy;
END;
$$;

CREATE TABLE IF NOT EXISTS hl_trade_registry (
    bucket SMALLINT NOT NULL,
    source TEXT NOT NULL,
    coin TEXT NOT NULL,
    tid BIGINT NOT NULL,
    PRIMARY KEY (bucket, source, coin, tid)
) PARTITION BY LIST (bucket);

DO $$
DECLARE bucket_id INTEGER;
BEGIN
    FOR bucket_id IN 0..31 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS hl_trade_registry_p%s PARTITION OF hl_trade_registry FOR VALUES IN (%s)',
            bucket_id,
            bucket_id
        );
    END LOOP;
END;
$$;

CREATE INDEX IF NOT EXISTS hl_trade_registry_lookup_idx
    ON hl_trade_registry (source, coin, tid);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'hl_trade_registry_legacy'
    ) THEN
        RETURN;
    END IF;
    INSERT INTO hl_trade_registry (bucket, source, coin, tid)
    SELECT hl_trade_registry_bucket(source, coin, tid), source, coin, tid
    FROM hl_trade_registry_legacy
    ON CONFLICT DO NOTHING;
    DROP TABLE hl_trade_registry_legacy;
END;
$$;

CREATE OR REPLACE FUNCTION hl_trades_register_tid() RETURNS trigger AS $$
BEGIN
    IF NEW.time IS NULL OR NEW.time <= TIMESTAMPTZ '1970-01-01 00:00:00+00' THEN
        PERFORM bronze_raise_check_violation('hl_trades_time_positive', 'hl_trades.time must be positive');
    END IF;
    IF NEW.side IS NULL OR NEW.side NOT IN ('A', 'B') THEN
        PERFORM bronze_raise_check_violation('hl_trades_side_valid', 'hl_trades.side must be A or B');
    END IF;
    IF NEW.price IS NULL OR NEW.price::text IN ('NaN', 'Infinity', '-Infinity') THEN
        PERFORM bronze_raise_check_violation('hl_trades_price_finite', 'hl_trades.price must be finite');
    END IF;
    IF NEW.size IS NULL OR NEW.size::text IN ('NaN', 'Infinity', '-Infinity') THEN
        PERFORM bronze_raise_check_violation('hl_trades_size_finite', 'hl_trades.size must be finite');
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
