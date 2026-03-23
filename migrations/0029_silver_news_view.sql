CREATE VIEW v_news_deduped (
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
WITH news_stats AS (
    SELECT dedup_url, count(*) AS article_count, count(DISTINCT source) AS source_count
    FROM news
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
    FROM news
    ORDER BY dedup_url, time, ingested_at, source, url
),
latest_sentiment AS (
    SELECT DISTINCT ON (dedup_url)
        dedup_url,
        source AS sentiment_source,
        sentiment_score,
        sentiment_label
    FROM news_sentiment
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
