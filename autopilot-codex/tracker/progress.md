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
