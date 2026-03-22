ALTER TABLE hl_trades DROP CONSTRAINT IF EXISTS hl_trades_tid_required;
ALTER TABLE hl_l2book DROP CONSTRAINT IF EXISTS hl_l2book_snapshot_hash_required;
ALTER TABLE hl_asset_ctx DROP CONSTRAINT IF EXISTS hl_asset_ctx_event_hash_required;
ALTER TABLE hl_candles DROP CONSTRAINT IF EXISTS hl_candles_event_hash_required;
ALTER TABLE bn_agg_trades DROP CONSTRAINT IF EXISTS bn_agg_trades_source_id_required;
ALTER TABLE bn_depth DROP CONSTRAINT IF EXISTS bn_depth_dedup_key_required;
ALTER TABLE bn_mark_price DROP CONSTRAINT IF EXISTS bn_mark_price_event_hash_required;
ALTER TABLE bn_liquidations DROP CONSTRAINT IF EXISTS bn_liquidations_event_hash_required;
ALTER TABLE bn_tickers DROP CONSTRAINT IF EXISTS bn_tickers_ws_dedup_key_required;
ALTER TABLE bn_klines DROP CONSTRAINT IF EXISTS bn_klines_ws_event_hash_required;
ALTER TABLE bn_rest_delivery DROP CONSTRAINT IF EXISTS bn_rest_delivery_delivery_time_required;

CREATE OR REPLACE FUNCTION bronze_raise_check_violation(
    constraint_name TEXT,
    message_text TEXT
) RETURNS VOID AS $$
BEGIN
    RAISE EXCEPTION '%', message_text
        USING ERRCODE = '23514',
              CONSTRAINT = constraint_name;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION hl_trades_register_tid() RETURNS trigger AS $$
BEGIN
    IF NEW.tid IS NULL THEN
        PERFORM bronze_raise_check_violation('hl_trades_tid_required', 'hl_trades.tid is required');
    END IF;
    INSERT INTO hl_trade_registry (source, coin, tid)
    VALUES (NEW.source, NEW.coin, NEW.tid)
    ON CONFLICT DO NOTHING;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bn_agg_trades_register_trade_id() RETURNS trigger AS $$
BEGIN
    IF NEW.source IN ('bn_ws_agg_trades', 'bn_rest_agg_trades') THEN
        IF NEW.agg_trade_id IS NULL THEN
            PERFORM bronze_raise_check_violation('bn_agg_trades_source_id_required', 'bn_agg_trades.agg_trade_id is required');
        END IF;
        INSERT INTO bn_agg_trade_registry (source, symbol, agg_trade_id)
        VALUES (NEW.source, NEW.symbol, NEW.agg_trade_id)
        ON CONFLICT DO NOTHING;
        IF NOT FOUND THEN
            RETURN NULL;
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.source = 'bn_rest_trades' THEN
        IF NEW.trade_id IS NULL THEN
            PERFORM bronze_raise_check_violation('bn_agg_trades_source_id_required', 'bn_agg_trades.trade_id is required');
        END IF;
        INSERT INTO bn_trade_registry (source, symbol, trade_id)
        VALUES (NEW.source, NEW.symbol, NEW.trade_id)
        ON CONFLICT DO NOTHING;
        IF NOT FOUND THEN
            RETURN NULL;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION hl_l2book_register_replay_key() RETURNS trigger AS $$
BEGIN
    IF NEW.snapshot_hash IS NULL THEN
        PERFORM bronze_raise_check_violation('hl_l2book_snapshot_hash_required', 'hl_l2book.snapshot_hash is required');
    END IF;
    IF NOT bronze_register_replay_key('hl_l2book', NEW.source, NEW.coin, '', NEW.snapshot_hash) THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION hl_asset_ctx_register_replay_key() RETURNS trigger AS $$
BEGIN
    IF NEW.event_hash IS NULL THEN
        PERFORM bronze_raise_check_violation('hl_asset_ctx_event_hash_required', 'hl_asset_ctx.event_hash is required');
    END IF;
    IF NOT bronze_register_replay_key('hl_asset_ctx', NEW.source, NEW.coin, '', NEW.event_hash) THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION hl_candles_register_replay_key() RETURNS trigger AS $$
BEGIN
    IF NEW.event_hash IS NULL THEN
        PERFORM bronze_raise_check_violation('hl_candles_event_hash_required', 'hl_candles.event_hash is required');
    END IF;
    IF NOT bronze_register_replay_key('hl_candles', NEW.source, NEW.coin, NEW.interval, NEW.event_hash) THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bn_depth_register_replay_key() RETURNS trigger AS $$
DECLARE
    replay_key TEXT;
BEGIN
    replay_key := COALESCE(NEW.final_update_id::text, NEW.last_update_id::text, NEW.snapshot_hash);
    IF replay_key IS NULL THEN
        PERFORM bronze_raise_check_violation('bn_depth_dedup_key_required', 'bn_depth replay key is required');
    END IF;
    IF NEW.source <> 'bn_ws_depth' THEN
        RETURN NEW;
    END IF;
    IF NOT bronze_register_replay_key('bn_depth', NEW.source, NEW.symbol, '', replay_key) THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bn_mark_price_register_replay_key() RETURNS trigger AS $$
BEGIN
    IF NEW.event_hash IS NULL THEN
        PERFORM bronze_raise_check_violation('bn_mark_price_event_hash_required', 'bn_mark_price.event_hash is required');
    END IF;
    IF NOT bronze_register_replay_key('bn_mark_price', NEW.source, NEW.symbol, '', NEW.event_hash) THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bn_liquidations_register_replay_key() RETURNS trigger AS $$
BEGIN
    IF NEW.event_hash IS NULL THEN
        PERFORM bronze_raise_check_violation('bn_liquidations_event_hash_required', 'bn_liquidations.event_hash is required');
    END IF;
    IF NOT bronze_register_replay_key('bn_liquidations', NEW.source, NEW.symbol, '', NEW.event_hash) THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bn_tickers_register_replay_key() RETURNS trigger AS $$
DECLARE
    replay_key TEXT;
BEGIN
    IF NEW.source = 'bn_ws_book_ticker' THEN
        IF NEW.update_id IS NULL THEN
            PERFORM bronze_raise_check_violation('bn_tickers_ws_dedup_key_required', 'bn_tickers.update_id is required');
        END IF;
        replay_key := NEW.update_id::text;
    ELSIF NEW.source IN ('bn_ws_ticker', 'bn_ws_mini_ticker') THEN
        IF NEW.event_hash IS NULL THEN
            PERFORM bronze_raise_check_violation('bn_tickers_ws_dedup_key_required', 'bn_tickers.event_hash is required');
        END IF;
        replay_key := NEW.event_hash;
    ELSE
        RETURN NEW;
    END IF;
    IF NOT bronze_register_replay_key('bn_tickers', NEW.source, NEW.symbol, '', replay_key) THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bn_klines_register_replay_key() RETURNS trigger AS $$
BEGIN
    IF NEW.source <> 'bn_ws_candles' THEN
        RETURN NEW;
    END IF;
    IF NEW.event_hash IS NULL THEN
        PERFORM bronze_raise_check_violation('bn_klines_ws_event_hash_required', 'bn_klines.event_hash is required');
    END IF;
    IF NOT bronze_register_replay_key('bn_klines', NEW.source, NEW.symbol, NEW.interval, NEW.event_hash) THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bn_rest_delivery_register_delivery_time() RETURNS trigger AS $$
DECLARE
    inserted_count INTEGER;
BEGIN
    IF NEW.delivery_time IS NULL THEN
        PERFORM bronze_raise_check_violation('bn_rest_delivery_delivery_time_required', 'bn_rest_delivery.delivery_time is required');
    END IF;
    INSERT INTO bn_rest_delivery_registry (source, pair, delivery_time)
    VALUES (NEW.source, NEW.pair, NEW.delivery_time)
    ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    IF inserted_count = 0 THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
