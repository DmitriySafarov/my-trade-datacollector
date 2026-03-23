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
