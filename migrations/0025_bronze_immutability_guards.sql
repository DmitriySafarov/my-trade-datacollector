CREATE OR REPLACE FUNCTION bronze_table_names() RETURNS TEXT[] AS $$
    SELECT ARRAY[
        'hl_trades',
        'hl_l2book',
        'hl_asset_ctx',
        'hl_candles',
        'hl_rest_candles',
        'hl_rest_funding',
        'bn_agg_trades',
        'bn_depth',
        'bn_mark_price',
        'bn_liquidations',
        'bn_tickers',
        'bn_klines',
        'bn_rest_funding',
        'bn_rest_oi',
        'bn_rest_oi_hist',
        'bn_rest_ls_top_pos',
        'bn_rest_ls_top_acc',
        'bn_rest_ls_global',
        'bn_rest_taker_vol',
        'bn_rest_premium',
        'bn_rest_basis',
        'bn_rest_funding_info',
        'bn_rest_delivery',
        'bn_rest_exchange_info',
        'bn_rest_constituents',
        'news',
        'news_sentiment',
        'fred_data',
        'fear_greed',
        'cc_exchange_vol',
        'cc_price',
        'finnhub_quotes',
        'market_status',
        's3_trades',
        's3_l2book'
    ];
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION bronze_reject_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION '% is immutable; % is not allowed on Bronze tables', TG_TABLE_NAME, TG_OP
        USING ERRCODE = 'P0001';
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
    target_table TEXT;
BEGIN
    FOREACH target_table IN ARRAY bronze_table_names() LOOP
        EXECUTE format(
            'DROP TRIGGER IF EXISTS %I ON %I',
            target_table || '_reject_mutation_before_change',
            target_table
        );
        EXECUTE format(
            'DROP TRIGGER IF EXISTS %I ON %I',
            target_table || '_reject_truncate_before_truncate',
            target_table
        );
        EXECUTE format(
            'CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON %I '
            'FOR EACH ROW EXECUTE FUNCTION bronze_reject_mutation()',
            target_table || '_reject_mutation_before_change',
            target_table
        );
        EXECUTE format(
            'CREATE TRIGGER %I BEFORE TRUNCATE ON %I '
            'FOR EACH STATEMENT EXECUTE FUNCTION bronze_reject_mutation()',
            target_table || '_reject_truncate_before_truncate',
            target_table
        );
    END LOOP;
END;
$$;
