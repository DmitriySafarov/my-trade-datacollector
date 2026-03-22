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
