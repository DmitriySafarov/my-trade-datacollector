CREATE TABLE IF NOT EXISTS bn_rest_premium (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (source = 'bn_rest_premium'),
    symbol TEXT NOT NULL,
    mark_price DOUBLE PRECISION,
    index_price DOUBLE PRECISION,
    estimated_settle_price DOUBLE PRECISION,
    premium DOUBLE PRECISION,
    next_funding_time TIMESTAMPTZ,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable('bn_rest_premium', 'time', if_not_exists => TRUE, create_default_indexes => FALSE, chunk_time_interval => INTERVAL '7 days');
CREATE INDEX IF NOT EXISTS bn_rest_premium_symbol_time_idx ON bn_rest_premium (symbol, time DESC);
CREATE INDEX IF NOT EXISTS bn_rest_premium_source_time_idx ON bn_rest_premium (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS bn_rest_premium_source_time_symbol_uidx ON bn_rest_premium (source, time, symbol);

CREATE TABLE IF NOT EXISTS bn_rest_basis (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (source = 'bn_rest_basis'),
    pair TEXT NOT NULL,
    contract_type TEXT NOT NULL DEFAULT '',
    period TEXT NOT NULL DEFAULT '',
    index_price DOUBLE PRECISION,
    futures_price DOUBLE PRECISION,
    basis DOUBLE PRECISION,
    basis_rate DOUBLE PRECISION,
    annualized_basis_rate DOUBLE PRECISION,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable('bn_rest_basis', 'time', if_not_exists => TRUE, create_default_indexes => FALSE, chunk_time_interval => INTERVAL '7 days');
CREATE INDEX IF NOT EXISTS bn_rest_basis_pair_time_idx ON bn_rest_basis (pair, time DESC);
CREATE INDEX IF NOT EXISTS bn_rest_basis_source_time_idx ON bn_rest_basis (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS bn_rest_basis_source_time_pair_period_uidx ON bn_rest_basis (source, time, pair, contract_type, period);

CREATE TABLE IF NOT EXISTS bn_rest_funding_info (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (source = 'bn_rest_funding_info'),
    symbol TEXT NOT NULL,
    adjusted_funding_rate_cap DOUBLE PRECISION,
    adjusted_funding_rate_floor DOUBLE PRECISION,
    funding_interval_hours INTEGER,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable('bn_rest_funding_info', 'time', if_not_exists => TRUE, create_default_indexes => FALSE, chunk_time_interval => INTERVAL '7 days');
CREATE INDEX IF NOT EXISTS bn_rest_funding_info_symbol_time_idx ON bn_rest_funding_info (symbol, time DESC);
CREATE INDEX IF NOT EXISTS bn_rest_funding_info_source_time_idx ON bn_rest_funding_info (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS bn_rest_funding_info_source_time_symbol_uidx ON bn_rest_funding_info (source, time, symbol);

CREATE TABLE IF NOT EXISTS bn_rest_delivery (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (source = 'bn_rest_delivery'),
    pair TEXT NOT NULL,
    delivery_time TIMESTAMPTZ,
    delivery_price DOUBLE PRECISION,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable('bn_rest_delivery', 'time', if_not_exists => TRUE, create_default_indexes => FALSE, chunk_time_interval => INTERVAL '7 days');
CREATE INDEX IF NOT EXISTS bn_rest_delivery_pair_time_idx ON bn_rest_delivery (pair, time DESC);
CREATE INDEX IF NOT EXISTS bn_rest_delivery_source_time_idx ON bn_rest_delivery (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS bn_rest_delivery_source_time_pair_uidx ON bn_rest_delivery (source, time, pair);

CREATE TABLE IF NOT EXISTS bn_rest_exchange_info (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (source = 'bn_rest_exchange_info'),
    symbol TEXT NOT NULL,
    pair TEXT,
    contract_type TEXT,
    status TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable('bn_rest_exchange_info', 'time', if_not_exists => TRUE, create_default_indexes => FALSE, chunk_time_interval => INTERVAL '7 days');
CREATE INDEX IF NOT EXISTS bn_rest_exchange_info_symbol_time_idx ON bn_rest_exchange_info (symbol, time DESC);
CREATE INDEX IF NOT EXISTS bn_rest_exchange_info_source_time_idx ON bn_rest_exchange_info (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS bn_rest_exchange_info_source_time_symbol_uidx ON bn_rest_exchange_info (source, time, symbol);

CREATE TABLE IF NOT EXISTS bn_rest_constituents (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (source = 'bn_rest_constituents'),
    symbol TEXT NOT NULL,
    pair TEXT,
    contract_type TEXT,
    constituents JSONB NOT NULL DEFAULT '[]'::jsonb,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable('bn_rest_constituents', 'time', if_not_exists => TRUE, create_default_indexes => FALSE, chunk_time_interval => INTERVAL '7 days');
CREATE INDEX IF NOT EXISTS bn_rest_constituents_symbol_time_idx ON bn_rest_constituents (symbol, time DESC);
CREATE INDEX IF NOT EXISTS bn_rest_constituents_source_time_idx ON bn_rest_constituents (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS bn_rest_constituents_source_time_symbol_uidx ON bn_rest_constituents (source, time, symbol);
