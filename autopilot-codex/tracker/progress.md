# Data Collector — Progress Log

<!-- Autopilot appends entries here after each completed task -->

## 2026-03-22

- Completed Phase 0 bootstrap base: `docker-compose.yml`, `Dockerfile`, `.env.example`, `requirements*.txt`, and a minimal Python 3.12 collector runtime.
- Added runtime scaffolding for config loading, structured logging, asyncpg pool creation, SQL migrations, dual-trigger batch writer, base collector interface, health endpoint, and graceful shutdown.
- Added bootstrap SQL migrations for `timescaledb` extension, `settings`, `source_watermarks`, `gap_events`, and `verification_reports`.
- Installed local SDK/runtime dependencies into `.venv` and verified imports for `aiohttp`, `asyncpg`, `feedparser`, `finnhub`, `fredapi`, and `hyperliquid`.
- Verified the stack end-to-end with `docker compose up -d --build`: PostgreSQL and collector are healthy, migrations applied, and runtime settings seeded.
- Remaining Phase 0 items: Bronze hypertables, Silver views, and compression policies.

## 2026-03-23

- Completed `P0.2` by finishing Bronze immutability enforcement and closing the DB-test bootstrap regressions uncovered in five verify loops.
- Added `migrations/0025_bronze_immutability_guards.sql` and `migrations/0026_bronze_immutability_registry.sql`, including a registry-backed `bronze_table_names()` and reusable `refresh_bronze_immutability_guards(...)` helper instead of rewriting an already-issued migration.
- Hardened the DB harness in `tests/_db_connection.py`, `tests/_db_test_support.py`, and `tests/conftest.py` so compose mode can try ordered socket/TCP transports while fixture consumers keep the exact selected connection config.
- Added focused regression coverage in `tests/test_db_connection.py`, `tests/test_db_transport_selection.py`, `tests/test_db_test_support.py`, `tests/db/test_bronze_schema.py`, and `tests/db/test_bronze_immutability.py`.
- Decision: keep `0025` unchanged and layer `0026` on top to avoid migration drift while still making future Bronze immutability refresh reusable.
- Issues: verify sandboxes that block both Docker access and local PostgreSQL transports still need the explicit external DB path, but the harness now supports that fallback cleanly.
- Next step: start `P0.3` Silver views so normalized read models can build on the now-locked Bronze layer.

- Completed `P0.3` by adding append-only Silver view migrations `0027` through `0031` for canonical trades, funding, orderbook, open interest, candles, and deduplicated news over Bronze.
- Added real-PostgreSQL regression coverage for Silver normalization, replay cleanup, Hyperliquid REST recovery visibility, repaired news identity handling, and `0026 -> head` migration upgrades in `tests/db/test_silver_*.py` and `tests/db/test_migration_upgrade_silver_views.py`.
- Hardened the Silver upgrade harness in `tests/db/_migration_upgrade.py` so migration subsets fail fast on missing files and teardown remains best-effort without masking cleanup work.
- Decisions: keep `v_trades` canonical to Hyperliquid plus Binance aggregate trades only; keep `v_news_deduped` source-agnostic at one row per resolved dedup URL; treat Hyperliquid REST funding/candles as gap-fill behind WS-preferred Silver rows.
- Issues: the task required five implementation/verify loops to reconcile replay-repair edge cases, Bronze news dedup semantics, and migration-upgrade coverage, but the final repo-wide validation is clean.
- Validation: `./.venv/bin/ruff check .`, `./.venv/bin/ruff format --check .`, and `./.venv/bin/python -m pytest tests/ -v` all passed with `115 passed`.
- Next step: start `P0.5` TimescaleDB compression policies now that Silver views are stable and fully covered by DB-backed tests.

- Completed `P1.1` by adding the Hyperliquid websocket transport stack in `src/collectors/hyperliquid/` with reconnect supervision, bounded open timeout, jittered backoff, generation-safe callback routing, bounded shutdown drain, and session wrapping around `hyperliquid.websocket_manager.WebsocketManager`.
- Added focused Hyperliquid regression coverage in `tests/collectors/hyperliquid/` for reconnect delivery, open-timeout recovery, overflow-triggered reconnect, silent session exits, shutdown drain behavior, and SDK adapter hooks.
- Tracker correction: marked stale `P0.5` complete because the existing migration chain and real DB-backed schema tests already prove compression policies are configured.
- Decisions: use the lower-level SDK websocket manager directly instead of `hyperliquid.info.Info`; preserve already-received callbacks across reconnect/shutdown races; treat queue overflow as a connection-level reconnect trigger instead of blocking the single SDK callback thread.
- Issues: the task needed five verify/repair loops to close reconnect, shutdown, timeout, overflow, and late-callback races; silent half-open detection remains deferred to the later reliability phase.
- Validation: `./.venv/bin/ruff check .`, `./.venv/bin/ruff format --check .`, and `./.venv/bin/python -m pytest tests/ -v` all passed with `137 passed`.
- Next step: start `P1.2` Hyperliquid trades collector on top of the now-stable WS manager.

- Completed `P1.2` by adding the Hyperliquid ETH/BTC trades collector in `src/collectors/hyperliquid/`, wiring runtime startup, and landing Bronze writes into `hl_trades` through a dedicated COPY-based storage helper.
- Added and expanded DB-backed coverage for Bronze landing, replay deduplication, malformed-envelope quarantine, invalid-row quarantine, reconnect persistence, parser bounds, batch-writer cancellation safety, and WS shutdown/cleanup timeout behavior across `tests/collectors/hyperliquid/`, `tests/test_batch_writer.py`, and `tests/test_runtime_collectors.py`.
- Added `migrations/0032_hl_trades_guards_registry.sql` to harden direct `hl_trades` inserts and move replay-key state off a single hot registry table into bucketed partitions.
- Decisions: keep the collector on the existing single Hyperliquid WS manager; quarantine malformed trade rows per-record instead of failing the stream; fail closed when a live Hyperliquid session cannot be stopped; keep shutdown bounded at the manager layer while still flushing the writer-owned Bronze batch.
- Issues: this task required five verify/repair loops to reconcile timer-flush cancellation, shutdown/reconnect semantics, parser range safety, DB guardrails, and realistic cleanup-timeout test coverage.
- Validation: `./.venv/bin/python -m ruff check .`, `./.venv/bin/python -m ruff format --check .`, and `./.venv/bin/python -m pytest tests/ -v` all passed with `171 passed`.
- Next step: start `P1.3` Hyperliquid L2Book collector on top of the now-hardened WS manager and replay-guard path.

- Completed `P1.3` by wiring the Hyperliquid ETH/BTC L2Book collector into the shared market websocket runtime and landing Bronze writes into `hl_l2book` with replay-safe storage, Decimal-based parsing, and runtime collector registration.
- Hardened the shared Hyperliquid lifecycle around the new collector with restart-safe run coordination, stale-callback fencing, shutdown intake barriers, bounded writer close/abort behavior, preserved disconnect diagnostics, and source-aware health snapshots across `src/collectors/hyperliquid/`, `src/db/batch_writer.py`, and the related test harness helpers.
- Added repair and guard migrations `0033`, `0034`, and `0035` so blank replay keys, invalid Hyperliquid trade values, and non-finite or zeroed Hyperliquid L2Book doubles are filtered out of registry rebuilds and Silver reads without mutating Bronze history.
- Expanded DB-backed and collector regression coverage across `tests/collectors/hyperliquid/`, `tests/db/`, `tests/test_batch_writer.py`, `tests/test_db_test_support.py`, and `tests/test_runtime_collectors.py`, including restart/stop races, reconnect diagnostics, blank-hash reconciliation, HL trade contracts, and L2Book overflow/underflow cases.
- Decisions: keep trades and L2Book on one Hyperliquid websocket manager; keep reconnect gap timestamps connection-scoped while leaving per-source freshness in `source_health`; compute L2Book derived values in `Decimal` space and only materialize checked floats at the final boundary.
- Issues: the task required five implementation/verify loops to close WS lifecycle races, shutdown hangs, replay/Silver repair leaks, DB bootstrap harness behavior, and numeric edge cases in both parser and DB guards.
- Validation: `./.venv/bin/ruff check .`, `./.venv/bin/ruff format --check .`, and `./.venv/bin/python -m pytest tests/ -v` all passed with `245 passed in 159.50s`.
- Next step: start `P1.4` Hyperliquid `activeAssetCtx` collection on top of the now-stable shared market websocket path.

## 2026-03-24

- Completed `P1.4` by validating and landing the Hyperliquid ETH/BTC `activeAssetCtx` WebSocket collector, which was fully implemented in uncommitted working-tree changes from a prior session.
- Components: parser (`asset_ctx_parsing.py`, 171 lines) with strict float validation, SHA-256 event hash for replay dedup, optional mid_price/impact_prices; COPY-based storage (`asset_ctx_storage.py`, 39 lines) into `hl_asset_ctx`; handler (`handle_asset_ctx_message`) in `market_handlers.py` with envelope unwrap and quarantine-on-parse-error; `activeAssetCtx:{coin}` subscriptions in `market_support.py`; asset_ctx writer, source ID `hl_ws_asset_ctx`, and health reporting in `market.py`; runtime registration with `enable_asset_ctx=True` in `collectors/__init__.py`.
- No new migrations needed — `hl_asset_ctx` Bronze table (migration 0003), replay guards (0019), Silver views `v_funding`/`v_oi` (0028), and compression policies all pre-existed.
- Added 3 DB-backed integration tests: flush-on-stop with full column verification, replay deduplication across reconnect, and timer-triggered flush. Updated runtime factory test to expect all 3 sources (`hl_ws_trades`, `hl_ws_l2book`, `hl_ws_asset_ctx`).
- Fixed Docker postgres health issue: 7 leftover test databases exhausting TimescaleDB background workers; cleaned up and confirmed stable restart.
- Decisions: no new code written beyond lint fixes; implementation correctly follows the shared WS manager + COPY storage + replay dedup pattern from trades (P1.2) and L2Book (P1.3).
- Validation: `./.venv/bin/ruff check .`, `./.venv/bin/ruff format --check .`, and `./.venv/bin/python -m pytest tests/ -v` all passed with `248 passed in 339.65s`.
- Next step: start `P1.5` Hyperliquid candles collector — 11 intervals × 2 coins = 22 subscriptions.

- Completed `P1.5` by implementing the Hyperliquid ETH/BTC candles WebSocket collector with 11 intervals × 2 coins = 22 subscriptions, landing Bronze writes into `hl_candles` through COPY-based storage with SHA-256 event hash replay dedup.
- Components: parser (`candle_parsing.py`, 196 lines) with strict OHLC range validation, cross-field time validation, deterministic interval ordering; COPY-based storage (`candle_storage.py`, 36 lines); handler (`handle_candle_message`) in `market_handlers.py`; candle subscriptions per coin × per interval in `market_support.py` with early interval validation; writer factory (`market_writers.py`, 69 lines) extracted to keep `market.py` under 200-line convention; runtime registration with `enable_candles=True` in `collectors/__init__.py`.
- No new migrations needed — `hl_candles` Bronze table (migration 0003), replay guards (0019), Silver view `v_candles` (0028), and compression policies all pre-existed.
- Added 5 DB-backed integration tests (flush-on-stop with full column verification, replay deduplication across reconnect, timer-triggered flush, open candle handling, 22-subscription registration) and 21 unit tests for parsing validation. Updated runtime factory test to expect all 4 sources.
- Hardened `require_message_data` to raise `ValueError` for missing `data` field instead of returning `None`. Removed DRY violation: SOURCE_ID constants now single-sourced from `market_handlers.py`. Replaced `object()` pool stub with `MagicMock(spec=asyncpg.Pool)` in runtime test.
- Decisions: `is_closed` heuristic based on `T != t` (Hyperliquid WS open candles repeat open_time as close_time — verified against live API); `VALID_INTERVALS` as ordered tuple for deterministic subscription order; `trades_count` optional (n field may be absent).
- Validation: `./.venv/bin/ruff check .`, `./.venv/bin/ruff format --check .`, and `./.venv/bin/python -m pytest tests/ -v` all passed with `277 passed in 320.99s`.
- Next step: start `P2.1` REST polling framework for scheduled async tasks.

- Completed `P2.1` by implementing the REST polling framework in `src/collectors/rest/` with `BaseRestPoller(BaseCollector)` and `SlidingWindowRateLimiter` — reusable infrastructure for all future REST-based collectors (Phases 2-7).
- Components: `base_poller.py` (188 lines) with async poll loop, exponential backoff on failures, startup failure threshold, health tracking, `_post_json`/`_get_json` helpers with automatic rate limiting, full `BaseCollector` lifecycle (`start`/`stop`/`wait_ready`/`health_snapshot`); `rate_limiter.py` (97 lines) with sliding-window weight tracking, async `acquire(weight)`, concurrent-safe via `asyncio.Lock`, optimal sleep calculation until oldest entry expires.
- No new migrations needed — framework is pure Python with no DB schema changes. DB integration tests verify the COPY write path through `hl_rest_candles` Bronze hypertable.
- Added 27 tests: 13 base poller tests (lifecycle, polling, error handling, backoff, rate limiter integration), 12 rate limiter tests (validation, weight tracking, expiration, concurrency, snapshot), 2 DB-backed integration tests (write to `hl_rest_candles`, health after write).
- Decisions: no separate `RestClient` class (YAGNI — helpers on `BaseRestPoller` suffice); `aiohttp.ClientSession` managed in `start()` context; shared rate limiter for same-API pollers; direct COPY writes (low-volume REST data, count_limit=1 is unnecessary overhead); `max_startup_failures=5` raises during startup, post-readiness errors are logged but non-fatal.
- Validation: `./.venv/bin/ruff check .`, `./.venv/bin/ruff format --check .`, and `./.venv/bin/python -m pytest tests/ -v` all passed with `304 passed in 165.97s`.
- Next step: start `P2.2` Candles snapshot collector — subclass `BaseRestPoller`, poll Hyperliquid REST `candleSnapshot` every 4h, write to `hl_rest_candles`.

- Completed `P2.2` by implementing `HyperliquidCandlesPoller` — the first concrete `BaseRestPoller` subclass — polling Hyperliquid REST `candleSnapshot` every 4h across 2 coins × 11 intervals, writing closed candles to `hl_rest_candles` Bronze hypertable with `INSERT ON CONFLICT DO NOTHING` dedup on `(source, time, coin, interval)`.
- Components: `hl_candles_parsing.py` (94 lines) delegates to shared WS candle parser, returns `None` for open candles, adapts to REST table schema (no `is_closed`/`event_hash`, `time = open_time`); `hl_candles_storage.py` (39 lines) with `INSERT ON CONFLICT DO NOTHING` via `executemany`; `hl_candles_poller.py` (161 lines) with coin × interval iteration, tiered error handling (0%/≤50%/>50%/100% failure thresholds), per-candle parse error tracking, and aggregated poll summary logging.
- No new migrations needed — `hl_rest_candles` Bronze table, unique index, and compression policy pre-existed from migration 0003.
- Added 13 unit tests for REST candle parsing (open candle → None, field mapping, validation rejections) and 12 poller tests (4 unit + 6 DB-backed integration + 2 error handling). Updated runtime factory test to verify rate limiter wiring.
- Hardened `BaseRestPoller._poll_loop` with three-path ready-event guarantee: (1) normal first success, (2) startup failure threshold, (3) CancelledError/BaseException. Added `_startup_error` field for forwarding startup failures to `wait_ready()` callers. Fixed flaky cancellation test with explicit `poll_entered` event.
- Wired `SlidingWindowRateLimiter(max_weight=1200, window_seconds=60)` in `build_runtime_collectors` factory. Documented lock-free read inconsistency in `snapshot()` and `available_weight()`.
- Decisions: `INSERT ON CONFLICT DO NOTHING` over COPY (REST dedup requires conflict handling); open candles return `None` (expected every poll, not an error); 24h lookback window ensures coverage across all interval sizes; parse errors tracked separately from fetch errors in poll summary.
- Issues: five verify/repair loops to close startup-failure deadlock, CancelledError deadlock, open-candle log spam, missing rate limiter wiring, flaky timing test, and parse error visibility in summary logs.
- Validation: `./.venv/bin/ruff check .`, `./.venv/bin/ruff format --check .`, and `./.venv/bin/python -m pytest tests/ -v` all passed with `331 passed in 176.42s`.
- Next step: start `P2.3` Funding history collector — every 8h (source: hl_rest_funding).

- Completed `P2.3` by validating and landing the Hyperliquid funding history REST collector, which was fully implemented in uncommitted working-tree changes from a prior session.
- Components: `hl_funding_parsing.py` (107 lines) with strict float validation, belt-and-suspenders `allowed_coins` guard, `as_row()` for DB writes; `hl_funding_storage.py` (38 lines) with `INSERT ON CONFLICT DO NOTHING`; `hl_funding_poller.py` (172 lines) with per-coin iteration, tiered error handling (0%/≥50%/100% failure thresholds), `total_parse_errors` tracked in health snapshots; runtime registration with shared `SlidingWindowRateLimiter(max_weight=1200)` in `build_runtime_collectors`.
- Hardened REST polling framework across 4 verify/repair loops: fixed degraded-threshold off-by-one (`>` → `>=`), added blocking-start docstring, replaced flaky sleep-based test with deterministic `poll_entered` event, strengthened `available_weight()` docstring, added explicit `API_WEIGHT` constant to candles poller, extracted poll loop into `_poll_loop.py` to keep `base_poller.py` under 200-line convention (227 → 147 lines), added `total_parse_errors` to health snapshots in both concrete pollers.
- No new migrations needed — `hl_rest_funding` Bronze table (migration 0003), unique index, and compression policy pre-existed.
- Added 13 unit tests for REST funding parsing and 9 poller tests (3 unit + 4 DB-backed integration + 2 error handling, including explicit 50% degraded threshold test).
- Validation: `./.venv/bin/ruff check .`, `./.venv/bin/ruff format --check .`, and `./.venv/bin/python -m pytest tests/ -v` all passed with `384 passed in 189s`.

- Completed `P3.1` by implementing the Binance Futures combined stream WebSocket connection manager from scratch in `src/collectors/binance/`.
- Components: `ws_types.py` (21 lines) with `StreamConfig`, `StreamHandler` type definitions; `ws_manager.py` (184 lines) with `BinanceWsManager` handling connection, reconnect with exponential backoff, stream URL building, lifecycle management; `ws_receive_support.py` (93 lines) with `receive_loop`, `handle_text`, `MessageStats` for message routing; `__init__.py` (12 lines) with module exports.
- Implements combined stream URL pattern `wss://fstream.binance.com/stream?streams=<s1>/<s2>/...` with `{"stream": "...", "data": {...}}` message routing by exact stream name match.
- Added 18 tests: constructor validation, URL building (single/multi stream, trailing slash), health snapshot, message routing (7 tests covering correct handler, unknown stream, missing fields, handler error, bad JSON, non-dict), lifecycle (5 tests covering connect+receive+stop, reconnect after disconnect, startup failure, wait_ready raises, CLOSE message handling).
- Key decisions: raw aiohttp WebSocket (no python-binance SDK dependency); handler errors are non-fatal (tracked in `total_routing_errors`); `stop()` closes WS to unblock receive loop; no DB integration (pure connection infrastructure for P3.2–P3.7).
- Validation: `./.venv/bin/ruff check .`, `./.venv/bin/ruff format --check .`, and `./.venv/bin/python -m pytest tests/ -v` all passed with `384 passed in 189s`.
- Next step: start `P3.2` Binance aggTrade collector — ETH + BTC (source: bn_ws_agg_trades).
