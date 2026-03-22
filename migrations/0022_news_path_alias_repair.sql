-- migrate: no-transaction
DROP FUNCTION IF EXISTS bronze_normalize_url_host(TEXT);
DROP FUNCTION IF EXISTS news_source_claims_dedup_url(TEXT, TEXT);
DROP FUNCTION IF EXISTS bronze_normalize_url_path(TEXT);
DROP FUNCTION IF EXISTS bronze_normalize_url_host_path(TEXT);
DROP TABLE IF EXISTS news_url_path_aliases;
DROP TABLE IF EXISTS news_url_host_path_aliases;
DROP FUNCTION IF EXISTS news_lookup_dedup_identity(TEXT, TEXT);
DROP FUNCTION IF EXISTS news_resolve_dedup_identity(TEXT, TEXT);
DROP FUNCTION IF EXISTS news_lookup_dedup_identity(TEXT, TEXT, TEXT);
DROP FUNCTION IF EXISTS news_resolve_dedup_identity(TEXT, TEXT, TEXT);
CREATE OR REPLACE FUNCTION bronze_normalize_url_host(target_url TEXT)
RETURNS TEXT AS $$
    SELECT lower(substring(bronze_normalize_url_identity(target_url) FROM '^[a-z]+://([^/?#]+)'))
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION news_source_claims_dedup_url(source_id TEXT, dedup_identity TEXT)
RETURNS BOOLEAN AS $$
    SELECT COALESCE(
        source_id IS NOT NULL
        AND dedup_identity IS NOT NULL
        AND (
            EXISTS (
                SELECT 1
                FROM news_dedup_registry
                WHERE source = source_id
                  AND dedup_url = dedup_identity
            )
            OR EXISTS (
                SELECT 1
                FROM news_sentiment_dedup_registry
                WHERE source = source_id
                  AND dedup_url = dedup_identity
            )
        ),
        FALSE
    )
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION news_lookup_dedup_identity(raw_url TEXT, canonical_url TEXT, source_id TEXT)
RETURNS TEXT AS $$
DECLARE
    raw_identity TEXT := bronze_normalize_url_identity(raw_url);
    canonical_identity TEXT := bronze_normalize_url_identity(canonical_url);
    raw_host TEXT := bronze_normalize_url_host(raw_url);
    canonical_host TEXT := bronze_normalize_url_host(canonical_url);
    raw_resolved TEXT;
    canonical_resolved TEXT;
BEGIN
    SELECT resolved_dedup_url INTO raw_resolved
    FROM news_url_aliases
    WHERE alias_url = raw_identity;

    SELECT resolved_dedup_url INTO canonical_resolved
    FROM news_url_aliases
    WHERE alias_url = canonical_identity;

    IF raw_identity IS NOT NULL
       AND canonical_identity IS NOT NULL
       AND raw_host IS DISTINCT FROM canonical_host
    THEN
        IF news_source_claims_dedup_url(source_id, raw_resolved) THEN
            RETURN raw_resolved;
        END IF;
        RETURN canonical_identity;
    END IF;

    IF news_source_claims_dedup_url(source_id, raw_resolved) THEN
        RETURN raw_resolved;
    END IF;
    IF news_source_claims_dedup_url(source_id, canonical_resolved) THEN
        RETURN canonical_resolved;
    END IF;
    RETURN COALESCE(canonical_identity, raw_identity);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION news_lookup_dedup_identity(raw_url TEXT, canonical_url TEXT)
RETURNS TEXT AS $$
BEGIN
    RETURN news_lookup_dedup_identity(raw_url, canonical_url, NULL::TEXT);
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

CREATE OR REPLACE FUNCTION news_resolve_dedup_identity(raw_url TEXT, canonical_url TEXT, source_id TEXT)
RETURNS TEXT AS $$
DECLARE
    raw_identity TEXT := bronze_normalize_url_identity(raw_url);
    canonical_identity TEXT := bronze_normalize_url_identity(canonical_url);
    resolved_identity TEXT;
BEGIN
    resolved_identity := news_lookup_dedup_identity(raw_url, canonical_url, source_id);
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
ALTER TABLE news_dedup_registry ADD COLUMN IF NOT EXISTS raw_host TEXT NOT NULL DEFAULT '';
ALTER TABLE news_sentiment_dedup_registry ADD COLUMN IF NOT EXISTS raw_host TEXT NOT NULL DEFAULT '';
ALTER TABLE news_dedup_registry DROP CONSTRAINT IF EXISTS news_dedup_registry_pkey;
ALTER TABLE news_sentiment_dedup_registry DROP CONSTRAINT IF EXISTS news_sentiment_dedup_registry_pkey;
CREATE UNIQUE INDEX IF NOT EXISTS news_dedup_registry_source_dedup_raw_host_uidx
    ON news_dedup_registry (source, dedup_url, raw_host);
CREATE UNIQUE INDEX IF NOT EXISTS news_sentiment_registry_source_dedup_raw_host_uidx
    ON news_sentiment_dedup_registry (source, dedup_url, raw_host);
TRUNCATE news_dedup_registry;
TRUNCATE news_sentiment_dedup_registry;
TRUNCATE news_provenance;
TRUNCATE news_url_aliases;
DO $$
DECLARE
    row RECORD;
    registry_url TEXT;
    raw_host TEXT;
BEGIN
    FOR row IN
        SELECT *
        FROM (
            SELECT 'news'::TEXT AS entry_type, url AS raw_url, canonical_url, ingested_at, time, source, source_name, payload FROM news
            UNION ALL
            SELECT 'sentiment'::TEXT AS entry_type, article_url AS raw_url, canonical_url, ingested_at, time, source, source_name, payload FROM news_sentiment
        ) AS url_events
        ORDER BY ingested_at, time, source, raw_url, canonical_url, entry_type
    LOOP
        registry_url := news_resolve_dedup_identity(row.raw_url, row.canonical_url, row.source);
        raw_host := COALESCE(bronze_normalize_url_host(row.raw_url), '');
        IF row.entry_type = 'news' THEN
            INSERT INTO news_provenance (dedup_url, source, source_name, first_seen_at, payload)
            VALUES (registry_url, row.source, row.source_name, row.ingested_at, row.payload)
            ON CONFLICT DO NOTHING;
            INSERT INTO news_dedup_registry (source, dedup_url, raw_host)
            VALUES (row.source, registry_url, raw_host)
            ON CONFLICT DO NOTHING;
        ELSE
            INSERT INTO news_sentiment_dedup_registry (source, dedup_url, raw_host)
            VALUES (row.source, registry_url, raw_host)
            ON CONFLICT DO NOTHING;
        END IF;
    END LOOP;
END
$$;
CREATE OR REPLACE FUNCTION news_register_dedup_url() RETURNS trigger AS $$
DECLARE
    inserted_count INTEGER;
    registry_url TEXT;
BEGIN
    registry_url := news_resolve_dedup_identity(NEW.url, NEW.canonical_url, NEW.source);
    INSERT INTO news_provenance (dedup_url, source, source_name, first_seen_at, payload)
    VALUES (registry_url, NEW.source, NEW.source_name, NEW.ingested_at, NEW.payload)
    ON CONFLICT DO NOTHING;
    INSERT INTO news_dedup_registry (source, dedup_url, raw_host)
    VALUES (NEW.source, registry_url, COALESCE(bronze_normalize_url_host(NEW.url), ''))
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
    registry_url := news_resolve_dedup_identity(NEW.article_url, NEW.canonical_url, NEW.source);
    INSERT INTO news_sentiment_dedup_registry (source, dedup_url, raw_host)
    VALUES (NEW.source, registry_url, COALESCE(bronze_normalize_url_host(NEW.article_url), ''))
    ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    IF inserted_count = 0 THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
