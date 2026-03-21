# Project Testing Rules

Use this file as the project-specific source of truth for collector tests.

## Rules From AGENTS.md

- Use real PostgreSQL via Docker, not mocks
- Test each collector independently
- Verify data lands in the correct Bronze table
- Verify the correct `source` ID is written
- Collector writes only to Bronze tables
- Timestamps must be UTC / `TIMESTAMPTZ`
- Shutdown must flush buffers on SIGTERM
- Reconnect paths must preserve idempotency via `ON CONFLICT DO NOTHING`

## Architecture Constraints

- Bronze is immutable and write-only from collectors
- Silver/Gold are downstream read/query layers; collector tests should focus on Bronze writes unless the task explicitly changes downstream logic
- High-frequency flows use buffered writes, so tests may need explicit flush calls

## What Good Coverage Looks Like

- One focused test for the changed behavior
- One persistence assertion against the target Bronze table
- One provenance assertion for `source`
- One timestamp/assertion for UTC-compatible storage
- One duplicate/idempotency assertion when identifiers or reconnects matter

## When To Add Lifecycle Coverage

Add lifecycle tests when the change affects:

- WebSocket reconnect behavior
- shutdown / cancellation handling
- startup recovery
- batch writer flush scheduling

## What To Avoid

- Mocking asyncpg or replacing the DB with fakes
- Asserting internal private methods when public collector flows can be exercised
- Giant integration tests that cover multiple unrelated collectors at once
- Snapshot-style tests that assert full rows when only a few invariants matter
