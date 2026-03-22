TRUNCATE hl_trade_registry;
INSERT INTO hl_trade_registry (source, coin, tid)
SELECT DISTINCT source, coin, tid
FROM hl_trades
WHERE tid IS NOT NULL;

TRUNCATE bn_agg_trade_registry;
INSERT INTO bn_agg_trade_registry (source, symbol, agg_trade_id)
SELECT DISTINCT source, symbol, agg_trade_id
FROM bn_agg_trades
WHERE source IN ('bn_ws_agg_trades', 'bn_rest_agg_trades')
  AND agg_trade_id IS NOT NULL;

TRUNCATE bn_trade_registry;
INSERT INTO bn_trade_registry (source, symbol, trade_id)
SELECT DISTINCT source, symbol, trade_id
FROM bn_agg_trades
WHERE source = 'bn_rest_trades'
  AND trade_id IS NOT NULL;
