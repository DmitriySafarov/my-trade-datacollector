CREATE TABLE IF NOT EXISTS s3_trades (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (source = 'hl_s3_trades'),
    coin TEXT NOT NULL,
    side TEXT,
    price DOUBLE PRECISION,
    size DOUBLE PRECISION,
    hash TEXT,
    tid BIGINT,
    wallet TEXT,
    dir TEXT,
    crossed BOOLEAN,
    start_position DOUBLE PRECISION,
    closed_pnl DOUBLE PRECISION,
    fee DOUBLE PRECISION,
    fee_token TEXT,
    twap_id TEXT,
    builder TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable(
    's3_trades',
    'time',
    if_not_exists => TRUE,
    create_default_indexes => FALSE,
    chunk_time_interval => INTERVAL '1 day'
);
CREATE INDEX IF NOT EXISTS s3_trades_coin_time_idx ON s3_trades (coin, time DESC);
CREATE INDEX IF NOT EXISTS s3_trades_source_time_idx ON s3_trades (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS s3_trades_source_time_coin_tid_hash_uidx
    ON s3_trades (source, time, coin, tid, hash)
    WHERE tid IS NOT NULL AND hash IS NOT NULL;

CREATE TABLE IF NOT EXISTS s3_l2book (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (source = 'hl_s3_l2book'),
    coin TEXT NOT NULL,
    bids JSONB NOT NULL DEFAULT '[]'::jsonb,
    asks JSONB NOT NULL DEFAULT '[]'::jsonb,
    snapshot_hash TEXT,
    best_bid_price DOUBLE PRECISION,
    best_ask_price DOUBLE PRECISION,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable(
    's3_l2book',
    'time',
    if_not_exists => TRUE,
    create_default_indexes => FALSE,
    chunk_time_interval => INTERVAL '7 days'
);
CREATE INDEX IF NOT EXISTS s3_l2book_coin_time_idx ON s3_l2book (coin, time DESC);
CREATE INDEX IF NOT EXISTS s3_l2book_source_time_idx ON s3_l2book (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS s3_l2book_source_time_coin_snapshot_uidx
    ON s3_l2book (source, time, coin, snapshot_hash)
    WHERE snapshot_hash IS NOT NULL;
