CREATE OR REPLACE VIEW v_news_deduped (
    time,
    source,
    source_name,
    title,
    url,
    canonical_url,
    dedup_url,
    summary,
    related_symbols,
    sentiment_score,
    sentiment_label,
    sentiment_source,
    duplicate_count,
    source_count
) AS
WITH
resolved_news AS (
    SELECT
        news.time,
        news.source,
        news.source_name,
        news.title,
        news.url,
        news.canonical_url,
        news.summary,
        news.related_symbols,
        news.sentiment_score,
        news.sentiment_label,
        news.sentiment_source,
        news.ingested_at,
        news_lookup_dedup_identity(news.url, news.canonical_url) AS dedup_url,
        COALESCE(
            NULLIF(btrim(news.source_name), ''),
            format('source:%s', news.source)
        ) AS publisher_name
    FROM news
),
news_stats AS (
    SELECT dedup_url, count(*) AS article_count, count(DISTINCT publisher_name) AS source_count
    FROM resolved_news
    GROUP BY dedup_url
),
primary_news AS (
    SELECT DISTINCT ON (dedup_url)
        time,
        source,
        source_name,
        title,
        url,
        canonical_url,
        dedup_url,
        summary,
        related_symbols,
        sentiment_score,
        sentiment_label,
        sentiment_source
    FROM resolved_news
    ORDER BY dedup_url, time, ingested_at, source, url
),
resolved_sentiment AS (
    SELECT
        news_sentiment.time,
        news_sentiment.source,
        news_sentiment.sentiment_score,
        news_sentiment.sentiment_label,
        news_sentiment.ingested_at,
        news_lookup_dedup_identity(news_sentiment.article_url, news_sentiment.canonical_url) AS dedup_url
    FROM news_sentiment
),
latest_sentiment AS (
    SELECT DISTINCT ON (dedup_url)
        dedup_url,
        source AS sentiment_source,
        sentiment_score,
        sentiment_label
    FROM resolved_sentiment
    ORDER BY dedup_url, time DESC, ingested_at DESC
)
SELECT
    primary_news.time,
    primary_news.source,
    primary_news.source_name,
    primary_news.title,
    primary_news.url,
    primary_news.canonical_url,
    primary_news.dedup_url,
    primary_news.summary,
    primary_news.related_symbols,
    COALESCE(latest_sentiment.sentiment_score, primary_news.sentiment_score) AS sentiment_score,
    COALESCE(latest_sentiment.sentiment_label, primary_news.sentiment_label) AS sentiment_label,
    COALESCE(latest_sentiment.sentiment_source, primary_news.sentiment_source) AS sentiment_source,
    GREATEST(news_stats.article_count - 1, 0) AS duplicate_count,
    news_stats.source_count
FROM primary_news
JOIN news_stats USING (dedup_url)
LEFT JOIN latest_sentiment USING (dedup_url);
