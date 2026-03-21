# Test Patterns

Use the smallest pattern that covers the changed behavior.

## Pattern 1: Fixture Replay Into Collector

Use when parser or message-handling logic changes.

1. Load one fixture from `tests/fixtures/`
2. Pass it through the collector entrypoint
3. Flush batched writes
4. Query the Bronze table
5. Assert `source`, key business fields, and timestamps

## Pattern 2: Duplicate / Idempotency Test

Use when the collector writes rows with stable IDs or reconnects may replay data.

1. Feed the same payload twice
2. Flush writes
3. Count rows by natural key or event ID
4. Assert only one persisted row when dedup is expected

## Pattern 3: Batch Writer Flush Test

Use when changing buffering thresholds or COPY flush behavior.

1. Queue enough records to trigger count-based flush, or advance the time-based path if the code supports it
2. Assert data is persisted after flush
3. Assert no duplicate persistence on repeated flush calls

## Pattern 4: Lifecycle Test

Use for WebSocket or long-running collectors.

1. Start the collector task
2. Feed or receive a small amount of data
3. Cancel or stop it cleanly
4. Flush any pending writes
5. Assert rows persisted and no obvious shutdown/reconnect regression

## Pattern 5: Regression Test

Use when fixing a known bug.

1. Build the smallest fixture that reproduces the old bug
2. Exercise the failing path
3. Assert the previous bad behavior no longer occurs
4. Keep the test narrowly tied to the regression
