CREATE TABLE IF NOT EXISTS news (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (
        source IN (
            'news_cryptocompare',
            'news_finnhub',
            'news_rss_coindesk',
            'news_rss_cointelegraph',
            'news_rss_decrypt',
            'news_rss_theblock',
            'news_finnhub_general',
            'news_finnhub_economy',
            'news_finnhub_business',
            'news_finnhub_coin',
            'news_finnhub_mstr'
        )
    ),
    source_name TEXT,
    title TEXT,
    url TEXT NOT NULL,
    canonical_url TEXT,
    dedup_url TEXT GENERATED ALWAYS AS (COALESCE(canonical_url, url)) STORED,
    summary TEXT,
    related_symbols JSONB NOT NULL DEFAULT '[]'::jsonb,
    sentiment_score DOUBLE PRECISION,
    sentiment_label TEXT,
    sentiment_source TEXT CHECK (
        sentiment_source IS NULL OR sentiment_source = 'sentiment_finbert'
    ),
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable(
    'news',
    'time',
    if_not_exists => TRUE,
    create_default_indexes => FALSE,
    chunk_time_interval => INTERVAL '7 days'
);
CREATE INDEX IF NOT EXISTS news_source_time_idx ON news (source, time DESC);
CREATE INDEX IF NOT EXISTS news_dedup_url_time_idx ON news (dedup_url, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS news_source_time_dedup_url_uidx
    ON news (source, time, dedup_url);

CREATE TABLE IF NOT EXISTS news_sentiment (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (source = 'sentiment_finbert'),
    article_url TEXT NOT NULL,
    canonical_url TEXT,
    dedup_url TEXT GENERATED ALWAYS AS (COALESCE(canonical_url, article_url)) STORED,
    source_name TEXT,
    title TEXT,
    sentiment_score DOUBLE PRECISION NOT NULL,
    sentiment_label TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable('news_sentiment', 'time', if_not_exists => TRUE, create_default_indexes => FALSE, chunk_time_interval => INTERVAL '7 days');
CREATE INDEX IF NOT EXISTS news_sentiment_source_time_idx ON news_sentiment (source, time DESC);
CREATE INDEX IF NOT EXISTS news_sentiment_dedup_url_time_idx ON news_sentiment (dedup_url, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS news_sentiment_time_dedup_url_uidx ON news_sentiment (time, dedup_url);

CREATE TABLE IF NOT EXISTS fred_data (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (
        source IN ('fred_release_dates', 'fred_daily', 'fred_monthly', 'fred_quarterly')
    ),
    event_type TEXT,
    series_id TEXT,
    release_id TEXT,
    release_name TEXT,
    units TEXT,
    value DOUBLE PRECISION,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable(
    'fred_data',
    'time',
    if_not_exists => TRUE,
    create_default_indexes => FALSE,
    chunk_time_interval => INTERVAL '7 days'
);
CREATE INDEX IF NOT EXISTS fred_data_series_time_idx ON fred_data (series_id, time DESC);
CREATE INDEX IF NOT EXISTS fred_data_source_time_idx ON fred_data (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS fred_data_source_time_series_uidx
    ON fred_data (source, time, series_id) WHERE series_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS fred_data_source_time_release_uidx
    ON fred_data (source, time, release_id) WHERE release_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS fear_greed (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (source = 'fear_greed'),
    value INTEGER,
    classification TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable(
    'fear_greed',
    'time',
    if_not_exists => TRUE,
    create_default_indexes => FALSE,
    chunk_time_interval => INTERVAL '7 days'
);
CREATE INDEX IF NOT EXISTS fear_greed_source_time_idx ON fear_greed (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS fear_greed_source_time_uidx ON fear_greed (source, time);

CREATE TABLE IF NOT EXISTS cc_exchange_vol (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (source = 'cc_exchange_vol'),
    exchange TEXT NOT NULL,
    coin TEXT,
    volume_usd DOUBLE PRECISION,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable(
    'cc_exchange_vol',
    'time',
    if_not_exists => TRUE,
    create_default_indexes => FALSE,
    chunk_time_interval => INTERVAL '7 days'
);
CREATE INDEX IF NOT EXISTS cc_exchange_vol_exchange_time_idx ON cc_exchange_vol (exchange, time DESC);
CREATE INDEX IF NOT EXISTS cc_exchange_vol_source_time_idx ON cc_exchange_vol (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS cc_exchange_vol_source_time_exchange_uidx
    ON cc_exchange_vol (source, time, exchange);

CREATE TABLE IF NOT EXISTS cc_price (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (source = 'cc_price_full'),
    coin TEXT NOT NULL,
    price DOUBLE PRECISION,
    market_cap DOUBLE PRECISION,
    circulating_supply DOUBLE PRECISION,
    total_supply DOUBLE PRECISION,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable(
    'cc_price',
    'time',
    if_not_exists => TRUE,
    create_default_indexes => FALSE,
    chunk_time_interval => INTERVAL '7 days'
);
CREATE INDEX IF NOT EXISTS cc_price_coin_time_idx ON cc_price (coin, time DESC);
CREATE INDEX IF NOT EXISTS cc_price_source_time_idx ON cc_price (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS cc_price_source_time_coin_uidx ON cc_price (source, time, coin);

CREATE TABLE IF NOT EXISTS finnhub_quotes (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (source = 'finnhub_quotes'),
    symbol TEXT NOT NULL,
    price DOUBLE PRECISION,
    change DOUBLE PRECISION,
    change_percent DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    open DOUBLE PRECISION,
    previous_close DOUBLE PRECISION,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable(
    'finnhub_quotes',
    'time',
    if_not_exists => TRUE,
    create_default_indexes => FALSE,
    chunk_time_interval => INTERVAL '7 days'
);
CREATE INDEX IF NOT EXISTS finnhub_quotes_symbol_time_idx ON finnhub_quotes (symbol, time DESC);
CREATE INDEX IF NOT EXISTS finnhub_quotes_source_time_idx ON finnhub_quotes (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS finnhub_quotes_source_time_symbol_uidx
    ON finnhub_quotes (source, time, symbol);

CREATE TABLE IF NOT EXISTS market_status (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (source = 'finnhub_market_status'),
    exchange TEXT NOT NULL DEFAULT 'US',
    is_open BOOLEAN,
    session TEXT,
    holiday_name TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable(
    'market_status',
    'time',
    if_not_exists => TRUE,
    create_default_indexes => FALSE,
    chunk_time_interval => INTERVAL '7 days'
);
CREATE INDEX IF NOT EXISTS market_status_exchange_time_idx ON market_status (exchange, time DESC);
CREATE INDEX IF NOT EXISTS market_status_source_time_idx ON market_status (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS market_status_source_time_exchange_uidx
    ON market_status (source, time, exchange);
