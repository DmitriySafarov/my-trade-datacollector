# Data Collector — Feature Tracker

## Phase 0: Infrastructure

- [x] P0.1: Docker Compose (PostgreSQL 16 + TimescaleDB + Python 3.12 service)
- [x] P0.2: Bronze tables — all 19 hypertables (hl_trades, hl_l2book, hl_asset_ctx, hl_candles, bn_agg_trades, bn_depth, bn_mark_price, bn_liquidations, bn_tickers, bn_klines, news, fred_data, fear_greed, cc_exchange_vol, cc_price, finnhub_quotes, market_status, s3_trades, s3_l2book)
- [x] P0.3: Silver views (v_trades, v_funding, v_orderbook, v_oi, v_candles, v_news_deduped)
- [x] P0.4: Settings table (database-backed config)
- [x] P0.5: TimescaleDB compression policies (48h raw → compressed)
- [x] P0.6: asyncpg connection pool + batch writer (dual trigger)
- [x] P0.7: Base collector class (start, stop, health check interface)
- [x] P0.8: Config loader (env vars for bootstrap, DB settings for runtime)
- [x] P0.9: Logging setup (structured, source ID in every record)
- [x] P0.10: Main entrypoint (startup sequence, graceful shutdown on SIGTERM)

## Phase 1: Hyperliquid WS

- [x] P1.1: WS connection manager (hyperliquid-python-sdk, reconnect with backoff)
- [x] P1.2: Trades collector — ETH + BTC (source: hl_ws_trades)
- [x] P1.3: L2Book collector — ETH + BTC, 20 levels (source: hl_ws_l2book)
- [x] P1.4: ActiveAssetCtx collector — ETH + BTC (source: hl_ws_asset_ctx)
- [ ] P1.5: Candles collector — 11 intervals × 2 coins = 22 subscriptions (source: hl_ws_candles)

## Phase 2: Hyperliquid REST

- [ ] P2.1: REST polling framework (scheduled async tasks)
- [ ] P2.2: Candles snapshot collector — every 4h (source: hl_rest_candles)
- [ ] P2.3: Funding history collector — every 8h (source: hl_rest_funding)

## Phase 3: Binance WS

- [ ] P3.1: Combined stream connection (wss://fstream.binance.com/stream?streams=..., reconnect with backoff)
- [ ] P3.2: aggTrade collector — ETH + BTC (source: bn_ws_agg_trades)
- [ ] P3.3: markPrice @1s collector — ETH + BTC (source: bn_ws_mark_price)
- [ ] P3.4: depth20 @100ms collector — ETH + BTC (source: bn_ws_depth)
- [ ] P3.5: forceOrder (liquidations) collector — ETH + BTC (source: bn_ws_liquidations)
- [ ] P3.6: kline 1m collector — ETH + BTC (source: bn_ws_candles)
- [ ] P3.7: ticker + miniTicker + bookTicker collectors (sources: bn_ws_ticker, bn_ws_mini_ticker, bn_ws_book_ticker)

## Phase 4: Binance REST

- [ ] P4.1: Rate-limited REST poller (2400 weight/min budget)
- [ ] P4.2: Open Interest — current every 5min + history hourly (sources: bn_rest_oi, bn_rest_oi_hist)
- [ ] P4.3: Long/Short ratios — 3 endpoints hourly (sources: bn_rest_ls_top_pos, bn_rest_ls_top_acc, bn_rest_ls_global)
- [ ] P4.4: Taker buy/sell volume hourly (source: bn_rest_taker_vol)
- [ ] P4.5: Premium index every 5min + klines hourly (sources: bn_rest_premium, bn_rest_premium_kl)
- [ ] P4.6: Mark/Index price klines hourly (sources: bn_rest_mark_kl, bn_rest_index_kl)
- [ ] P4.7: Ticker + Book ticker + Price every 5min (sources: bn_rest_ticker_24h, bn_rest_book_ticker, bn_rest_price)
- [ ] P4.8: Klines with taker_buy_volume every 5min (source: bn_rest_klines)
- [ ] P4.9: Continuous klines hourly (source: bn_rest_cont_kl)
- [ ] P4.10: Order book depth every 30s (source: bn_rest_depth)
- [ ] P4.11: Basis hourly (source: bn_rest_basis)
- [ ] P4.12: Daily endpoints — exchange info, funding info, constituents, delivery (sources: bn_rest_exchange_info, bn_rest_funding_info, bn_rest_constituents, bn_rest_delivery)

## Phase 5: News & Sentiment

- [ ] P5.1: CryptoCompare news collector every 5min (source: news_cryptocompare)
- [ ] P5.2: Finnhub crypto news every 10min (source: news_finnhub)
- [ ] P5.3: Finnhub general/economy/business news (sources: news_finnhub_general, news_finnhub_economy, news_finnhub_business)
- [ ] P5.4: Finnhub company news — COIN + MSTR every 30min (sources: news_finnhub_coin, news_finnhub_mstr)
- [ ] P5.5: RSS collector — 4 feeds every 5min (sources: news_rss_coindesk, news_rss_cointelegraph, news_rss_decrypt, news_rss_theblock)
- [ ] P5.6: News deduplication by URL
- [ ] P5.7: FinBERT sentiment processor (source: sentiment_finbert)
- [ ] P5.8: Fear & Greed every 1h (source: fear_greed)
- [ ] P5.9: Finnhub stock quotes every 5min — COIN, MSTR, SPY (source: finnhub_quotes)
- [ ] P5.10: Finnhub market status every 15min (source: finnhub_market_status)

## Phase 6: FRED Macro

- [ ] P6.1: Release dates collector — 6 releases, daily (source: fred_release_dates)
- [ ] P6.2: Daily macro series — 14 series, daily ~22:00 UTC (source: fred_daily)
- [ ] P6.3: Monthly/quarterly series — 14 series, on publication (sources: fred_monthly, fred_quarterly)

## Phase 7: Market Analytics

- [ ] P7.1: CryptoCompare exchange volume — 5 exchanges, hourly (source: cc_exchange_vol)
- [ ] P7.2: CryptoCompare price aggregate — ETH + BTC, every 5min (source: cc_price_full)

## Phase 8: Reliability & S3

- [ ] P8.1: Health monitor — gap detection per source with thresholds
- [ ] P8.2: HTTP /health endpoint (per-source last update time)
- [ ] P8.3: Gap recovery — REST backfill for recoverable sources
- [ ] P8.4: Startup recovery — check last record, backfill gaps
- [ ] P8.5: S3 node fills backfill — daily cron (source: hl_s3_trades)
- [ ] P8.6: S3 L2Book archive backfill (source: hl_s3_l2book)
- [ ] P8.7: Data verification — WS vs S3 trades, WS vs REST candles/funding
- [ ] P8.8: Alerting — structured logs, later Telegram
