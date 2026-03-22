DO $$
DECLARE hypertable_name TEXT;
BEGIN
    FOREACH hypertable_name IN ARRAY ARRAY[
        'hl_rest_candles', 'hl_rest_funding', 'bn_rest_funding', 'bn_rest_oi',
        'bn_rest_oi_hist', 'bn_rest_ls_top_pos', 'bn_rest_ls_top_acc',
        'bn_rest_ls_global', 'bn_rest_taker_vol', 'bn_rest_premium',
        'bn_rest_basis', 'bn_rest_funding_info', 'bn_rest_delivery',
        'bn_rest_exchange_info', 'bn_rest_constituents', 'news',
        'news_sentiment', 'fred_data', 'fear_greed', 'cc_exchange_vol',
        'cc_price', 'finnhub_quotes', 'market_status'
    ] LOOP
        EXECUTE format(
            'SELECT remove_compression_policy(%L, if_exists => TRUE)',
            hypertable_name
        );
        EXECUTE format(
            'SELECT add_compression_policy(%L, compress_after => INTERVAL ''48 hours'', if_not_exists => TRUE)',
            hypertable_name
        );
    END LOOP;
END
$$;
