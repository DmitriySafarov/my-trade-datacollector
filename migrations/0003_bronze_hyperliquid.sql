CREATE TABLE IF NOT EXISTS hl_trades (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (source = 'hl_ws_trades'),
    coin TEXT NOT NULL,
    side TEXT,
    price DOUBLE PRECISION,
    size DOUBLE PRECISION,
    hash TEXT,
    tid BIGINT,
    users JSONB NOT NULL DEFAULT '[]'::jsonb,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable('hl_trades', 'time', if_not_exists => TRUE, create_default_indexes => FALSE, chunk_time_interval => INTERVAL '1 day');
CREATE INDEX IF NOT EXISTS hl_trades_coin_time_idx ON hl_trades (coin, time DESC);
CREATE INDEX IF NOT EXISTS hl_trades_source_time_idx ON hl_trades (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS hl_trades_source_time_coin_tid_uidx
    ON hl_trades (source, time, coin, tid) WHERE tid IS NOT NULL;

CREATE TABLE IF NOT EXISTS hl_l2book (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (source = 'hl_ws_l2book'),
    coin TEXT NOT NULL,
    bids JSONB NOT NULL DEFAULT '[]'::jsonb,
    asks JSONB NOT NULL DEFAULT '[]'::jsonb,
    snapshot_hash TEXT,
    best_bid_price DOUBLE PRECISION,
    best_bid_size DOUBLE PRECISION,
    best_ask_price DOUBLE PRECISION,
    best_ask_size DOUBLE PRECISION,
    spread DOUBLE PRECISION,
    bid_notional DOUBLE PRECISION,
    ask_notional DOUBLE PRECISION,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable('hl_l2book', 'time', if_not_exists => TRUE, create_default_indexes => FALSE, chunk_time_interval => INTERVAL '1 day');
CREATE INDEX IF NOT EXISTS hl_l2book_coin_time_idx ON hl_l2book (coin, time DESC);
CREATE INDEX IF NOT EXISTS hl_l2book_source_time_idx ON hl_l2book (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS hl_l2book_source_time_coin_snapshot_uidx
    ON hl_l2book (source, time, coin, snapshot_hash) WHERE snapshot_hash IS NOT NULL;

CREATE TABLE IF NOT EXISTS hl_asset_ctx (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (source = 'hl_ws_asset_ctx'),
    coin TEXT NOT NULL,
    funding DOUBLE PRECISION,
    open_interest DOUBLE PRECISION,
    mark_price DOUBLE PRECISION,
    oracle_price DOUBLE PRECISION,
    mid_price DOUBLE PRECISION,
    premium DOUBLE PRECISION,
    buy_impact_price DOUBLE PRECISION,
    sell_impact_price DOUBLE PRECISION,
    day_notional_volume DOUBLE PRECISION,
    prev_day_price DOUBLE PRECISION,
    event_hash TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable('hl_asset_ctx', 'time', if_not_exists => TRUE, create_default_indexes => FALSE, chunk_time_interval => INTERVAL '1 day');
CREATE INDEX IF NOT EXISTS hl_asset_ctx_coin_time_idx ON hl_asset_ctx (coin, time DESC);
CREATE INDEX IF NOT EXISTS hl_asset_ctx_source_time_idx ON hl_asset_ctx (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS hl_asset_ctx_source_time_coin_event_uidx
    ON hl_asset_ctx (source, time, coin, event_hash) WHERE event_hash IS NOT NULL;

CREATE TABLE IF NOT EXISTS hl_candles (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (source = 'hl_ws_candles'),
    coin TEXT NOT NULL,
    interval TEXT NOT NULL,
    open_time TIMESTAMPTZ NOT NULL,
    close_time TIMESTAMPTZ,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    trades_count BIGINT,
    is_closed BOOLEAN NOT NULL DEFAULT FALSE,
    event_hash TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable('hl_candles', 'time', if_not_exists => TRUE, create_default_indexes => FALSE, chunk_time_interval => INTERVAL '1 day');
CREATE INDEX IF NOT EXISTS hl_candles_coin_interval_time_idx ON hl_candles (coin, interval, time DESC);
CREATE INDEX IF NOT EXISTS hl_candles_source_time_idx ON hl_candles (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS hl_candles_ws_event_uidx
    ON hl_candles (source, time, coin, interval, event_hash)
    WHERE event_hash IS NOT NULL;

CREATE TABLE IF NOT EXISTS hl_rest_candles (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (source = 'hl_rest_candles'),
    coin TEXT NOT NULL,
    interval TEXT NOT NULL,
    open_time TIMESTAMPTZ NOT NULL,
    close_time TIMESTAMPTZ,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    trades_count BIGINT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (time = open_time)
);
SELECT create_hypertable('hl_rest_candles', 'time', if_not_exists => TRUE, create_default_indexes => FALSE, chunk_time_interval => INTERVAL '7 days');
CREATE INDEX IF NOT EXISTS hl_rest_candles_coin_interval_time_idx ON hl_rest_candles (coin, interval, time DESC);
CREATE INDEX IF NOT EXISTS hl_rest_candles_source_time_idx ON hl_rest_candles (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS hl_rest_candles_source_time_coin_interval_uidx
    ON hl_rest_candles (source, time, coin, interval);

CREATE TABLE IF NOT EXISTS hl_rest_funding (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (source = 'hl_rest_funding'),
    coin TEXT NOT NULL,
    funding DOUBLE PRECISION,
    premium DOUBLE PRECISION,
    mark_price DOUBLE PRECISION,
    oracle_price DOUBLE PRECISION,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable('hl_rest_funding', 'time', if_not_exists => TRUE, create_default_indexes => FALSE, chunk_time_interval => INTERVAL '7 days');
CREATE INDEX IF NOT EXISTS hl_rest_funding_coin_time_idx ON hl_rest_funding (coin, time DESC);
CREATE INDEX IF NOT EXISTS hl_rest_funding_source_time_idx ON hl_rest_funding (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS hl_rest_funding_source_time_coin_uidx
    ON hl_rest_funding (source, time, coin);
