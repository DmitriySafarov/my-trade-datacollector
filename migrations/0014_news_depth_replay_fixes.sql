DELETE FROM bronze_replay_registry
WHERE table_name = 'bn_depth'
  AND source = 'bn_rest_depth';

CREATE TABLE IF NOT EXISTS news_provenance (
    dedup_url TEXT NOT NULL,
    source TEXT NOT NULL,
    source_name TEXT,
    first_seen_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (dedup_url, source)
);

INSERT INTO news_provenance (dedup_url, source, source_name, first_seen_at, payload)
SELECT DISTINCT ON (dedup_url, source)
    dedup_url,
    source,
    source_name,
    ingested_at,
    payload
FROM news
ORDER BY dedup_url, source, ingested_at, time
ON CONFLICT DO NOTHING;

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
    inserted_count INTEGER;
BEGIN
    registry_url := NEW.url;
    INSERT INTO news_provenance (dedup_url, source, source_name, first_seen_at, payload)
    VALUES (registry_url, NEW.source, NEW.source_name, NEW.ingested_at, NEW.payload)
    ON CONFLICT DO NOTHING;

    INSERT INTO news_dedup_registry (source, dedup_url)
    VALUES (NEW.source, registry_url)
    ON CONFLICT DO NOTHING;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    IF inserted_count = 0 THEN
        RETURN NULL;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

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
    inserted_count INTEGER;
BEGIN
    registry_url := NEW.article_url;
    INSERT INTO news_sentiment_dedup_registry (source, dedup_url)
    VALUES (NEW.source, registry_url)
    ON CONFLICT DO NOTHING;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    IF inserted_count = 0 THEN
        RETURN NULL;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bn_depth_register_replay_key() RETURNS trigger AS $$
DECLARE
    replay_key TEXT;
BEGIN
    IF NEW.source <> 'bn_ws_depth' THEN
        RETURN NEW;
    END IF;

    replay_key := COALESCE(
        NEW.final_update_id::text,
        NEW.last_update_id::text,
        NEW.snapshot_hash
    );
    IF replay_key IS NULL THEN
        RETURN NEW;
    END IF;

    IF NOT bronze_register_replay_key('bn_depth', NEW.source, NEW.symbol, '', replay_key) THEN
        RETURN NULL;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
