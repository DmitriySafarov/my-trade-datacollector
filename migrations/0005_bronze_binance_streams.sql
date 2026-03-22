CREATE TABLE IF NOT EXISTS bn_liquidations (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (source = 'bn_ws_liquidations'),
    symbol TEXT NOT NULL,
    side TEXT,
    price DOUBLE PRECISION,
    avg_price DOUBLE PRECISION,
    quantity DOUBLE PRECISION,
    filled_quantity DOUBLE PRECISION,
    trade_time TIMESTAMPTZ,
    order_type TEXT,
    time_in_force TEXT,
    status TEXT,
    event_hash TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable('bn_liquidations', 'time', if_not_exists => TRUE, create_default_indexes => FALSE, chunk_time_interval => INTERVAL '1 day');
CREATE INDEX IF NOT EXISTS bn_liquidations_symbol_time_idx ON bn_liquidations (symbol, time DESC);
CREATE INDEX IF NOT EXISTS bn_liquidations_source_time_idx ON bn_liquidations (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS bn_liquidations_source_time_symbol_event_uidx
    ON bn_liquidations (source, time, symbol, event_hash) WHERE event_hash IS NOT NULL;

CREATE TABLE IF NOT EXISTS bn_tickers (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (
        source IN (
            'bn_ws_ticker',
            'bn_ws_mini_ticker',
            'bn_ws_book_ticker',
            'bn_rest_ticker_24h',
            'bn_rest_price',
            'bn_rest_book_ticker'
        )
    ),
    symbol TEXT NOT NULL,
    update_id BIGINT,
    event_type TEXT,
    price DOUBLE PRECISION,
    last_price DOUBLE PRECISION,
    bid_price DOUBLE PRECISION,
    bid_qty DOUBLE PRECISION,
    ask_price DOUBLE PRECISION,
    ask_qty DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    quote_volume DOUBLE PRECISION,
    open_time TIMESTAMPTZ,
    close_time TIMESTAMPTZ,
    trade_count BIGINT,
    event_hash TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable('bn_tickers', 'time', if_not_exists => TRUE, create_default_indexes => FALSE, chunk_time_interval => INTERVAL '1 day');
CREATE INDEX IF NOT EXISTS bn_tickers_symbol_time_idx ON bn_tickers (symbol, time DESC);
CREATE INDEX IF NOT EXISTS bn_tickers_source_time_idx ON bn_tickers (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS bn_tickers_source_time_symbol_update_uidx
    ON bn_tickers (source, time, symbol, update_id) WHERE update_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS bn_tickers_source_time_symbol_event_uidx
    ON bn_tickers (source, time, symbol, event_hash) WHERE event_hash IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS bn_tickers_rest_source_time_symbol_uidx
    ON bn_tickers (source, time, symbol)
    WHERE source IN ('bn_rest_ticker_24h', 'bn_rest_price', 'bn_rest_book_ticker');

CREATE TABLE IF NOT EXISTS bn_klines (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (
        source IN (
            'bn_ws_candles',
            'bn_rest_klines',
            'bn_rest_premium_kl',
            'bn_rest_mark_kl',
            'bn_rest_index_kl',
            'bn_rest_cont_kl'
        )
    ),
    symbol TEXT NOT NULL,
    pair TEXT NOT NULL DEFAULT '',
    interval TEXT NOT NULL,
    contract_type TEXT NOT NULL DEFAULT '',
    open_time TIMESTAMPTZ NOT NULL,
    close_time TIMESTAMPTZ,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    quote_volume DOUBLE PRECISION,
    trade_count BIGINT,
    taker_buy_base_volume DOUBLE PRECISION,
    taker_buy_quote_volume DOUBLE PRECISION,
    is_closed BOOLEAN NOT NULL DEFAULT FALSE,
    event_hash TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (source = 'bn_ws_candles' OR time = open_time)
);
SELECT create_hypertable('bn_klines', 'time', if_not_exists => TRUE, create_default_indexes => FALSE, chunk_time_interval => INTERVAL '1 day');
CREATE INDEX IF NOT EXISTS bn_klines_symbol_interval_time_idx ON bn_klines (symbol, interval, time DESC);
CREATE INDEX IF NOT EXISTS bn_klines_source_time_idx ON bn_klines (source, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS bn_klines_rest_open_time_uidx
    ON bn_klines (source, time, symbol, interval, contract_type, pair)
    WHERE source <> 'bn_ws_candles';
CREATE UNIQUE INDEX IF NOT EXISTS bn_klines_ws_event_uidx
    ON bn_klines (source, time, symbol, interval, event_hash)
    WHERE source = 'bn_ws_candles' AND event_hash IS NOT NULL;
