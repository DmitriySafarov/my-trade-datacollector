-- migrate: no-transaction
CREATE OR REPLACE FUNCTION bronze_normalize_url_identity(target_url TEXT)
RETURNS TEXT AS $$
    SELECT regexp_replace(split_part(btrim(target_url), '#', 1), '/+$', '')
$$ LANGUAGE sql IMMUTABLE STRICT;
CREATE TABLE IF NOT EXISTS news_url_aliases (
    alias_url TEXT PRIMARY KEY,
    resolved_dedup_url TEXT NOT NULL
);
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'news_url_aliases'
          AND column_name = 'resolved_url'
    ) THEN
        EXECUTE 'ALTER TABLE news_url_aliases RENAME COLUMN resolved_url TO resolved_dedup_url';
    END IF;
END
$$;
CREATE OR REPLACE VIEW news_dedup_aliases AS SELECT alias_url, resolved_dedup_url AS primary_dedup_url FROM news_url_aliases;
CREATE OR REPLACE FUNCTION news_lookup_dedup_identity(raw_url TEXT, canonical_url TEXT)
RETURNS TEXT AS $$
DECLARE
    raw_identity TEXT := bronze_normalize_url_identity(raw_url);
    canonical_identity TEXT := bronze_normalize_url_identity(canonical_url);
    resolved_identity TEXT;
BEGIN
    SELECT resolved_dedup_url INTO resolved_identity
    FROM news_url_aliases
    WHERE alias_url IN (raw_identity, canonical_identity)
    ORDER BY CASE
        WHEN alias_url = raw_identity THEN 0
        WHEN alias_url = canonical_identity THEN 1
        ELSE 2
    END
    LIMIT 1;
    RETURN COALESCE(resolved_identity, canonical_identity, raw_identity);
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION news_resolve_dedup_identity(raw_url TEXT, canonical_url TEXT)
RETURNS TEXT AS $$
DECLARE
    raw_identity TEXT := bronze_normalize_url_identity(raw_url);
    canonical_identity TEXT := bronze_normalize_url_identity(canonical_url);
    resolved_identity TEXT;
BEGIN
    resolved_identity := news_lookup_dedup_identity(raw_url, canonical_url);
    IF raw_identity IS NOT NULL THEN
        INSERT INTO news_url_aliases (alias_url, resolved_dedup_url)
        VALUES (raw_identity, resolved_identity)
        ON CONFLICT DO NOTHING;
    END IF;
    IF canonical_identity IS NOT NULL THEN
        INSERT INTO news_url_aliases (alias_url, resolved_dedup_url)
        VALUES (canonical_identity, resolved_identity)
        ON CONFLICT DO NOTHING;
    END IF;
    RETURN resolved_identity;
END;
$$ LANGUAGE plpgsql;
TRUNCATE news_url_aliases;
DO $$
DECLARE row RECORD;
BEGIN
    FOR row IN
        SELECT raw_url, canonical_url, ingested_at, time, source
        FROM (
            SELECT url AS raw_url, canonical_url, ingested_at, time, source FROM news
            UNION ALL
            SELECT article_url AS raw_url, canonical_url, ingested_at, time, source FROM news_sentiment
        ) AS url_events
        ORDER BY ingested_at, time, source, raw_url, canonical_url
    LOOP
        PERFORM news_resolve_dedup_identity(row.raw_url, row.canonical_url);
    END LOOP;
END
$$;
DROP INDEX IF EXISTS news_source_time_dedup_url_uidx;
CREATE INDEX IF NOT EXISTS news_source_time_dedup_url_idx
    ON news (source, time DESC, dedup_url);
DROP INDEX IF EXISTS news_sentiment_time_dedup_url_uidx;
CREATE INDEX IF NOT EXISTS news_sentiment_time_dedup_url_idx
    ON news_sentiment (time DESC, dedup_url);
TRUNCATE news_dedup_registry;
INSERT INTO news_dedup_registry (source, dedup_url)
SELECT DISTINCT source, news_lookup_dedup_identity(url, canonical_url)
FROM news;
TRUNCATE news_sentiment_dedup_registry;
INSERT INTO news_sentiment_dedup_registry (source, dedup_url)
SELECT DISTINCT source, news_lookup_dedup_identity(article_url, canonical_url)
FROM news_sentiment;
TRUNCATE news_provenance;
INSERT INTO news_provenance (dedup_url, source, source_name, first_seen_at, payload)
SELECT DISTINCT ON (dedup_url, source)
    dedup_url,
    source,
    source_name,
    ingested_at,
    payload
FROM (
    SELECT
        news_lookup_dedup_identity(url, canonical_url) AS dedup_url,
        source,
        source_name,
        ingested_at,
        time,
        payload
    FROM news
) AS normalized_news
ORDER BY dedup_url, source, ingested_at, time;
CREATE OR REPLACE FUNCTION news_register_dedup_url() RETURNS trigger AS $$
DECLARE
    inserted_count INTEGER;
    registry_url TEXT;
BEGIN
    registry_url := news_resolve_dedup_identity(NEW.url, NEW.canonical_url);
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
CREATE OR REPLACE FUNCTION news_sentiment_register_dedup_url() RETURNS trigger AS $$
DECLARE
    inserted_count INTEGER;
    registry_url TEXT;
BEGIN
    registry_url := news_resolve_dedup_identity(NEW.article_url, NEW.canonical_url);
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
