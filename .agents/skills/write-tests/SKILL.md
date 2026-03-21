---
name: dc-write-tests
description: >
  Write or update tests for data collectors, parser changes, batch-writer changes,
  and regression fixes using real PostgreSQL and fixture replay. Activate when
  implementing a new collector, changing collector parsing/storage behavior, adding
  migrations that affect Bronze writes, or fixing bugs that need regression coverage.
  Tests must verify Bronze landing, correct source provenance, UTC timestamps,
  batching/idempotency behavior, and collector lifecycle semantics where applicable.
---

# Write Tests

Write tests from the spec and project invariants, not from the current implementation.

## Start Here

Read these files before writing collector tests:
- `AGENTS.md`
- `../plans/data-collector-spec-v2.md`
- `../plans/data-collector-reliability.md`
- `references/project-testing.md`
- `references/test-patterns.md`

If one of them is unavailable, continue and state the limitation.

## Autopilot Mode

- Do not ask questions during autonomous runs
- Pick the smallest correct test set that covers the changed behavior
- If live fixture capture is unavailable, use existing checked-in fixtures first
- If no real fixture is available, create the smallest realistic fixture that matches the documented payload shape and explicitly record that limitation
- If a required validation step is blocked, say exactly what was blocked and continue with the best available coverage

## Core Rules

1. Use real PostgreSQL via Docker; never mock the database
2. Prefer replaying real recorded API/WS payloads over invented payloads
3. Test from the spec and project invariants, not from current code structure
4. Verify behavior through public collector/batch-writer flows, not by asserting internal implementation details
5. Keep tests scoped to one collector or one storage behavior at a time

## Required Assertions

Every collector-related test suite should cover the relevant subset of:

- Data lands in the correct Bronze table
- `source` is correct
- timestamps are UTC / `TIMESTAMPTZ`
- expected columns are populated
- batch flush occurs correctly
- duplicates are ignored or deduplicated correctly
- shutdown/restart/reconnect behavior is preserved when applicable

## Test Selection

Choose tests based on the change:

- Parser/storage change: fixture replay + DB assertions
- Batch writer change: flush threshold/time trigger + duplicate/idempotency assertions
- WebSocket collector change: message handling + reconnect/dedup + graceful shutdown
- REST collector change: polling response parsing + DB landing + provenance assertions
- Regression bug fix: one focused regression test reproducing the old failure

## Fixture Strategy

Use this fallback order:

1. Existing checked-in real fixture
2. Newly captured real payload
3. Minimal realistic fixture shaped from source docs or recorded logs when capture is impossible

When using fallback 3, document the limitation in the task log or review output.

Store fixtures in `tests/fixtures/` using source-specific names.

## Test Structure

Prefer tests that follow this flow:

1. Load fixture
2. Feed collector or storage path
3. Flush pending writes if needed
4. Query the Bronze table directly
5. Assert provenance, timestamps, populated columns, and dedup semantics

See `references/test-patterns.md` for collector-specific patterns.

## Validation

After writing tests:

- Run the narrowest relevant pytest target first
- Then run broader affected test scopes if time allows
- Run `ruff check` on changed test files when available

If validation cannot run, say exactly what was blocked.
