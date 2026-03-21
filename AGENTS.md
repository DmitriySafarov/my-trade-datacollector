# Data Collector Service

## Project

Autonomous 24/7 service collecting ALL available crypto market data into PostgreSQL + TimescaleDB.
Not connected to trading bots. Bots and analytics read from this database.

Full specification: `../plans/data-collector-spec-v2.md`
Reliability spec: `../plans/data-collector-reliability.md`

## Stack

- Python 3.12, asyncio, single event loop
- asyncpg (raw SQL, no ORM, COPY for batch inserts)
- aiohttp (HTTP/WS client)
- hyperliquid-python-sdk (Hyperliquid WS + REST)
- finnhub-python (Finnhub SDK)
- fredapi (FRED SDK)
- feedparser (RSS)
- PostgreSQL 16 + TimescaleDB
- Docker Compose

## Architecture

### Medallion (Bronze → Silver → Gold)

- **Bronze**: per-source tables, immutable, write-only. Collector writes ONLY here
- **Silver**: PostgreSQL views (UNION across exchanges, dedup, normalize)
- **Gold**: materialized views, precomputed indicators (future)

### Data Flow

```
WS/REST → Collector → Buffer (dual trigger) → asyncpg COPY → Bronze tables
                                                                    ↓
                                                              Silver views
                                                                    ↓
                                                              Gold mat.views
```

### Batch Writer

Dual trigger: flush at count OR time limit (whichever first).
High-freq WS: 500 records / 2 sec. Low-freq REST: immediate.

### WebSocket

2 connections total:
1. Hyperliquid — SDK, 28 subscriptions
2. Binance — combined stream, 16 subscriptions

## Rules

- Every record MUST have `source` field (provenance). See spec for 60 source IDs
- ALL timestamps UTC. TIMESTAMPTZ in PostgreSQL
- No ORM. Raw SQL via asyncpg
- No YAML config. Database-backed settings (env vars only for bootstrap)
- Async everywhere. No blocking calls
- Files under 200 lines. Split if exceeded
- Graceful shutdown: flush buffers on SIGTERM
- ON CONFLICT DO NOTHING for idempotency on WS reconnect

## Structure

```
data-collector/
├── src/
│   ├── collectors/
│   │   ├── hyperliquid/    # WS + REST collectors
│   │   ├── binance/        # WS (combined) + REST collectors
│   │   ├── news/           # CryptoCompare, Finnhub, RSS, FinBERT
│   │   ├── fred/           # FRED releases + series
│   │   ├── analytics/      # CC vol/price, Finnhub quotes, market status
│   │   ├── s3/             # S3 backfill (node fills, l2book)
│   │   └── sentiment/      # FinBERT inference
│   ├── db/                 # asyncpg pool, batch writer, migrations runner
│   ├── health/             # Health monitor, gap detection, /health endpoint
│   ├── recovery/           # Gap recovery, startup recovery
│   ├── config/             # Env loader, DB settings reader
│   └── api/                # Future: REST API for frontend
├── migrations/             # Numbered SQL files
├── tests/
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

## Testing

- Real PostgreSQL (Docker), no mocks
- Test each collector independently
- Verify data lands in correct Bronze table with correct source ID
- Integration test: start collector → wait N seconds → check DB has records

## Feature Tracker

Source of truth for autopilot: `autopilot/tracker/feature-tracker.md`
