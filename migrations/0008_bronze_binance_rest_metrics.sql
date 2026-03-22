CREATE TABLE IF NOT EXISTS bn_rest_funding (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (source = 'bn_rest_funding'),
    symbol TEXT NOT NULL,
    funding_rate DOUBLE PRECISION,
    mark_price DOUBLE PRECISION,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable('bn_rest_funding', 'time', if_not_exists => TRUE, create_default_indexes => FALSE, chunk_time_interval => INTERVAL '7 days');
CREATE INDEX IF NOT EXISTS bn_rest_funding_symbol_time_idx ON bn_rest_funding (symbol, time DESC);
CREATE INDEX IF NOT EXISTS bn_rest_funding_source_time_idx ON bn_rest_funding (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS bn_rest_funding_source_time_symbol_uidx ON bn_rest_funding (source, time, symbol);

CREATE TABLE IF NOT EXISTS bn_rest_oi (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (source = 'bn_rest_oi'),
    symbol TEXT NOT NULL,
    open_interest DOUBLE PRECISION,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable('bn_rest_oi', 'time', if_not_exists => TRUE, create_default_indexes => FALSE, chunk_time_interval => INTERVAL '7 days');
CREATE INDEX IF NOT EXISTS bn_rest_oi_symbol_time_idx ON bn_rest_oi (symbol, time DESC);
CREATE INDEX IF NOT EXISTS bn_rest_oi_source_time_idx ON bn_rest_oi (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS bn_rest_oi_source_time_symbol_uidx ON bn_rest_oi (source, time, symbol);

CREATE TABLE IF NOT EXISTS bn_rest_oi_hist (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (source = 'bn_rest_oi_hist'),
    symbol TEXT NOT NULL,
    period TEXT NOT NULL DEFAULT '',
    open_interest DOUBLE PRECISION,
    open_interest_value DOUBLE PRECISION,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable('bn_rest_oi_hist', 'time', if_not_exists => TRUE, create_default_indexes => FALSE, chunk_time_interval => INTERVAL '7 days');
CREATE INDEX IF NOT EXISTS bn_rest_oi_hist_symbol_time_idx ON bn_rest_oi_hist (symbol, time DESC);
CREATE INDEX IF NOT EXISTS bn_rest_oi_hist_source_time_idx ON bn_rest_oi_hist (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS bn_rest_oi_hist_source_time_symbol_period_uidx ON bn_rest_oi_hist (source, time, symbol, period);

CREATE TABLE IF NOT EXISTS bn_rest_ls_top_pos (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (source = 'bn_rest_ls_top_pos'),
    symbol TEXT NOT NULL,
    period TEXT NOT NULL DEFAULT '',
    long_short_ratio DOUBLE PRECISION,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable('bn_rest_ls_top_pos', 'time', if_not_exists => TRUE, create_default_indexes => FALSE, chunk_time_interval => INTERVAL '7 days');
CREATE INDEX IF NOT EXISTS bn_rest_ls_top_pos_symbol_time_idx ON bn_rest_ls_top_pos (symbol, time DESC);
CREATE INDEX IF NOT EXISTS bn_rest_ls_top_pos_source_time_idx ON bn_rest_ls_top_pos (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS bn_rest_ls_top_pos_source_time_symbol_period_uidx ON bn_rest_ls_top_pos (source, time, symbol, period);

CREATE TABLE IF NOT EXISTS bn_rest_ls_top_acc (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (source = 'bn_rest_ls_top_acc'),
    symbol TEXT NOT NULL,
    period TEXT NOT NULL DEFAULT '',
    long_short_ratio DOUBLE PRECISION,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable('bn_rest_ls_top_acc', 'time', if_not_exists => TRUE, create_default_indexes => FALSE, chunk_time_interval => INTERVAL '7 days');
CREATE INDEX IF NOT EXISTS bn_rest_ls_top_acc_symbol_time_idx ON bn_rest_ls_top_acc (symbol, time DESC);
CREATE INDEX IF NOT EXISTS bn_rest_ls_top_acc_source_time_idx ON bn_rest_ls_top_acc (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS bn_rest_ls_top_acc_source_time_symbol_period_uidx ON bn_rest_ls_top_acc (source, time, symbol, period);

CREATE TABLE IF NOT EXISTS bn_rest_ls_global (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (source = 'bn_rest_ls_global'),
    symbol TEXT NOT NULL,
    period TEXT NOT NULL DEFAULT '',
    long_short_ratio DOUBLE PRECISION,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable('bn_rest_ls_global', 'time', if_not_exists => TRUE, create_default_indexes => FALSE, chunk_time_interval => INTERVAL '7 days');
CREATE INDEX IF NOT EXISTS bn_rest_ls_global_symbol_time_idx ON bn_rest_ls_global (symbol, time DESC);
CREATE INDEX IF NOT EXISTS bn_rest_ls_global_source_time_idx ON bn_rest_ls_global (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS bn_rest_ls_global_source_time_symbol_period_uidx ON bn_rest_ls_global (source, time, symbol, period);

CREATE TABLE IF NOT EXISTS bn_rest_taker_vol (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (source = 'bn_rest_taker_vol'),
    symbol TEXT NOT NULL,
    contract_type TEXT NOT NULL DEFAULT '',
    period TEXT NOT NULL DEFAULT '',
    buy_volume DOUBLE PRECISION,
    sell_volume DOUBLE PRECISION,
    buy_sell_ratio DOUBLE PRECISION,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable('bn_rest_taker_vol', 'time', if_not_exists => TRUE, create_default_indexes => FALSE, chunk_time_interval => INTERVAL '7 days');
CREATE INDEX IF NOT EXISTS bn_rest_taker_vol_symbol_time_idx ON bn_rest_taker_vol (symbol, time DESC);
CREATE INDEX IF NOT EXISTS bn_rest_taker_vol_source_time_idx ON bn_rest_taker_vol (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS bn_rest_taker_vol_source_time_symbol_period_uidx ON bn_rest_taker_vol (source, time, symbol, contract_type, period);
