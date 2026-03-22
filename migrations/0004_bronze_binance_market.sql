CREATE TABLE IF NOT EXISTS bn_agg_trades (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('bn_ws_agg_trades', 'bn_rest_trades', 'bn_rest_agg_trades')),
    symbol TEXT NOT NULL,
    trade_id BIGINT,
    agg_trade_id BIGINT,
    first_trade_id BIGINT,
    last_trade_id BIGINT,
    price DOUBLE PRECISION,
    qty DOUBLE PRECISION,
    is_buyer_maker BOOLEAN,
    is_best_match BOOLEAN,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable('bn_agg_trades', 'time', if_not_exists => TRUE, create_default_indexes => FALSE, chunk_time_interval => INTERVAL '1 day');
CREATE INDEX IF NOT EXISTS bn_agg_trades_symbol_time_idx ON bn_agg_trades (symbol, time DESC);
CREATE INDEX IF NOT EXISTS bn_agg_trades_source_time_idx ON bn_agg_trades (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS bn_agg_trades_source_time_symbol_agg_uidx
    ON bn_agg_trades (source, time, symbol, agg_trade_id) WHERE agg_trade_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS bn_agg_trades_source_time_symbol_trade_uidx
    ON bn_agg_trades (source, time, symbol, trade_id) WHERE trade_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS bn_depth (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('bn_ws_depth', 'bn_rest_depth')),
    symbol TEXT NOT NULL,
    first_update_id BIGINT,
    final_update_id BIGINT,
    last_update_id BIGINT,
    bids JSONB NOT NULL DEFAULT '[]'::jsonb,
    asks JSONB NOT NULL DEFAULT '[]'::jsonb,
    snapshot_hash TEXT,
    best_bid_price DOUBLE PRECISION,
    best_ask_price DOUBLE PRECISION,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable('bn_depth', 'time', if_not_exists => TRUE, create_default_indexes => FALSE, chunk_time_interval => INTERVAL '1 day');
CREATE INDEX IF NOT EXISTS bn_depth_symbol_time_idx ON bn_depth (symbol, time DESC);
CREATE INDEX IF NOT EXISTS bn_depth_source_time_idx ON bn_depth (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS bn_depth_source_time_symbol_final_uidx
    ON bn_depth (source, time, symbol, final_update_id) WHERE final_update_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS bn_depth_source_time_symbol_last_uidx
    ON bn_depth (source, time, symbol, last_update_id) WHERE last_update_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS bn_depth_source_time_symbol_snapshot_uidx
    ON bn_depth (source, time, symbol, snapshot_hash) WHERE snapshot_hash IS NOT NULL;

CREATE TABLE IF NOT EXISTS bn_mark_price (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (source = 'bn_ws_mark_price'),
    symbol TEXT NOT NULL,
    mark_price DOUBLE PRECISION,
    index_price DOUBLE PRECISION,
    estimated_settle_price DOUBLE PRECISION,
    premium DOUBLE PRECISION,
    funding_rate DOUBLE PRECISION,
    next_funding_time TIMESTAMPTZ,
    event_hash TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable('bn_mark_price', 'time', if_not_exists => TRUE, create_default_indexes => FALSE, chunk_time_interval => INTERVAL '1 day');
CREATE INDEX IF NOT EXISTS bn_mark_price_symbol_time_idx ON bn_mark_price (symbol, time DESC);
CREATE INDEX IF NOT EXISTS bn_mark_price_source_time_idx ON bn_mark_price (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS bn_mark_price_source_time_symbol_event_uidx
    ON bn_mark_price (source, time, symbol, event_hash) WHERE event_hash IS NOT NULL;
