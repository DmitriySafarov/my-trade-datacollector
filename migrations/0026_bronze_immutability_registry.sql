CREATE TABLE IF NOT EXISTS bronze_table_registry (
    table_schema TEXT NOT NULL,
    table_name TEXT NOT NULL,
    PRIMARY KEY (table_schema, table_name)
);

INSERT INTO bronze_table_registry (table_schema, table_name)
VALUES
    ('public', 'hl_trades'),
    ('public', 'hl_l2book'),
    ('public', 'hl_asset_ctx'),
    ('public', 'hl_candles'),
    ('public', 'hl_rest_candles'),
    ('public', 'hl_rest_funding'),
    ('public', 'bn_agg_trades'),
    ('public', 'bn_depth'),
    ('public', 'bn_mark_price'),
    ('public', 'bn_liquidations'),
    ('public', 'bn_tickers'),
    ('public', 'bn_klines'),
    ('public', 'bn_rest_funding'),
    ('public', 'bn_rest_oi'),
    ('public', 'bn_rest_oi_hist'),
    ('public', 'bn_rest_ls_top_pos'),
    ('public', 'bn_rest_ls_top_acc'),
    ('public', 'bn_rest_ls_global'),
    ('public', 'bn_rest_taker_vol'),
    ('public', 'bn_rest_premium'),
    ('public', 'bn_rest_basis'),
    ('public', 'bn_rest_funding_info'),
    ('public', 'bn_rest_delivery'),
    ('public', 'bn_rest_exchange_info'),
    ('public', 'bn_rest_constituents'),
    ('public', 'news'),
    ('public', 'news_sentiment'),
    ('public', 'fred_data'),
    ('public', 'fear_greed'),
    ('public', 'cc_exchange_vol'),
    ('public', 'cc_price'),
    ('public', 'finnhub_quotes'),
    ('public', 'market_status'),
    ('public', 's3_trades'),
    ('public', 's3_l2book')
ON CONFLICT DO NOTHING;

CREATE OR REPLACE FUNCTION bronze_table_names() RETURNS TEXT[] AS $$
    SELECT COALESCE(
        ARRAY_AGG(table_name ORDER BY table_name),
        ARRAY[]::TEXT[]
    )
    FROM bronze_table_registry
    WHERE table_schema = 'public';
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION refresh_bronze_immutability_guards(
    target_schema TEXT DEFAULT 'public',
    target_tables TEXT[] DEFAULT bronze_table_names()
) RETURNS VOID AS $$
DECLARE
    target_table TEXT;
BEGIN
    FOREACH target_table IN ARRAY target_tables LOOP
        IF to_regclass(format('%I.%I', target_schema, target_table)) IS NULL THEN
            RAISE EXCEPTION 'Bronze table %.% is not present', target_schema, target_table
                USING ERRCODE = 'P0001';
        END IF;
        EXECUTE format(
            'DROP TRIGGER IF EXISTS %I ON %I.%I',
            target_table || '_reject_mutation_before_change',
            target_schema,
            target_table
        );
        EXECUTE format(
            'DROP TRIGGER IF EXISTS %I ON %I.%I',
            target_table || '_reject_truncate_before_truncate',
            target_schema,
            target_table
        );
        EXECUTE format(
            'CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON %I.%I '
            'FOR EACH ROW EXECUTE FUNCTION bronze_reject_mutation()',
            target_table || '_reject_mutation_before_change',
            target_schema,
            target_table
        );
        EXECUTE format(
            'CREATE TRIGGER %I BEFORE TRUNCATE ON %I.%I '
            'FOR EACH STATEMENT EXECUTE FUNCTION bronze_reject_mutation()',
            target_table || '_reject_truncate_before_truncate',
            target_schema,
            target_table
        );
    END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT refresh_bronze_immutability_guards();
