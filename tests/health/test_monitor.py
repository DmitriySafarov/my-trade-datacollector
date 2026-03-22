from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from src.health.monitor import HealthMonitor


def _is_between(
    value: datetime,
    *,
    lower: datetime,
    upper: datetime,
) -> bool:
    return lower <= value <= upper


async def _db_now(pool) -> datetime:
    async with pool.acquire() as connection:
        return await connection.fetchval("SELECT CURRENT_TIMESTAMP")


@pytest.mark.asyncio
async def test_record_event_keeps_latest_watermark(
    migrated_db: dict[str, object],
) -> None:
    monitor = HealthMonitor(migrated_db["pool"])
    latest = datetime(2026, 3, 22, 0, 0, tzinfo=timezone.utc)
    earlier = datetime(2026, 3, 21, 23, 55, tzinfo=timezone.utc)

    await monitor.record_event("bn_ws_depth", latest, {"seq": 2})
    first_row = next(
        item for item in await monitor.snapshot() if item["source"] == "bn_ws_depth"
    )
    lower = await _db_now(migrated_db["pool"])
    await monitor.record_event("bn_ws_depth", earlier, {"seq": 1})
    upper = await _db_now(migrated_db["pool"])

    rows = await monitor.snapshot()
    row = next(item for item in rows if item["source"] == "bn_ws_depth")

    assert row["last_event_at"] == latest
    assert row["details"] == {"seq": 2}
    assert row["last_success_at"] > first_row["last_success_at"]
    assert row["updated_at"] > first_row["updated_at"]
    assert _is_between(row["last_success_at"], lower=lower, upper=upper)
    assert _is_between(row["updated_at"], lower=lower, upper=upper)


@pytest.mark.asyncio
async def test_record_event_refreshes_freshness_for_stale_events(
    migrated_db: dict[str, object],
) -> None:
    monitor = HealthMonitor(migrated_db["pool"])
    latest = datetime(2026, 3, 22, 0, 0, tzinfo=timezone.utc)
    earlier = datetime(2026, 3, 21, 23, 55, tzinfo=timezone.utc)

    await monitor.record_event("bn_ws_depth", latest, {"seq": 2})
    first_row = next(
        item for item in await monitor.snapshot() if item["source"] == "bn_ws_depth"
    )
    await asyncio.sleep(0.01)
    lower = await _db_now(migrated_db["pool"])
    await monitor.record_event("bn_ws_depth", earlier, {"seq": 1})
    upper = await _db_now(migrated_db["pool"])
    second_row = next(
        item for item in await monitor.snapshot() if item["source"] == "bn_ws_depth"
    )

    assert second_row["last_event_at"] == latest
    assert second_row["details"] == {"seq": 2}
    assert second_row["last_success_at"] > first_row["last_success_at"]
    assert second_row["updated_at"] > first_row["updated_at"]
    assert _is_between(second_row["last_success_at"], lower=lower, upper=upper)
    assert _is_between(second_row["updated_at"], lower=lower, upper=upper)


@pytest.mark.asyncio
async def test_record_event_refreshes_freshness_for_equal_timestamp_replays(
    migrated_db: dict[str, object],
) -> None:
    monitor = HealthMonitor(migrated_db["pool"])
    event_time = datetime(2026, 3, 22, 0, 0, tzinfo=timezone.utc)

    await monitor.record_event("bn_ws_depth", event_time, {"seq": 2})
    first_row = next(
        item for item in await monitor.snapshot() if item["source"] == "bn_ws_depth"
    )
    await asyncio.sleep(0.01)
    lower = await _db_now(migrated_db["pool"])
    await monitor.record_event("bn_ws_depth", event_time, {"seq": 3})
    upper = await _db_now(migrated_db["pool"])
    second_row = next(
        item for item in await monitor.snapshot() if item["source"] == "bn_ws_depth"
    )

    assert second_row["last_event_at"] == event_time
    assert second_row["details"] == {"seq": 2}
    assert second_row["last_success_at"] > first_row["last_success_at"]
    assert second_row["updated_at"] > first_row["updated_at"]
    assert _is_between(second_row["last_success_at"], lower=lower, upper=upper)
    assert _is_between(second_row["updated_at"], lower=lower, upper=upper)


@pytest.mark.asyncio
async def test_record_event_uses_processing_time_for_initial_freshness(
    migrated_db: dict[str, object],
) -> None:
    monitor = HealthMonitor(migrated_db["pool"])
    stale_event = datetime(2020, 1, 1, 0, 0, tzinfo=timezone.utc)
    lower = await _db_now(migrated_db["pool"])

    await monitor.record_event("bn_ws_depth", stale_event, {"seq": 1})
    upper = await _db_now(migrated_db["pool"])

    row = next(
        item for item in await monitor.snapshot() if item["source"] == "bn_ws_depth"
    )
    assert row["last_event_at"] == stale_event
    assert _is_between(row["last_success_at"], lower=lower, upper=upper)
    assert _is_between(row["updated_at"], lower=lower, upper=upper)


@pytest.mark.asyncio
async def test_record_event_never_moves_freshness_backwards(
    migrated_db: dict[str, object],
) -> None:
    monitor = HealthMonitor(migrated_db["pool"])
    first_event = datetime(2026, 3, 22, 0, 0, tzinfo=timezone.utc)
    second_event = datetime(2026, 3, 22, 0, 5, tzinfo=timezone.utc)
    pinned = datetime(2030, 1, 1, 0, 0, tzinfo=timezone.utc)

    await monitor.record_event("bn_ws_depth", first_event, {"seq": 1})
    async with migrated_db["pool"].acquire() as connection:
        await connection.execute(
            """
            UPDATE source_watermarks
            SET last_success_at = $2,
                updated_at = $2
            WHERE source = $1
            """,
            "bn_ws_depth",
            pinned,
        )

    await monitor.record_event("bn_ws_depth", second_event, {"seq": 2})
    row = next(
        item for item in await monitor.snapshot() if item["source"] == "bn_ws_depth"
    )

    assert row["last_event_at"] == second_event
    assert row["last_success_at"] == pinned
    assert row["updated_at"] == pinned


@pytest.mark.asyncio
async def test_record_event_normalizes_to_utc_and_rejects_naive_datetimes(
    migrated_db: dict[str, object],
) -> None:
    monitor = HealthMonitor(migrated_db["pool"])
    local_event = datetime.fromisoformat("2026-03-22T06:00:00+06:00")

    await monitor.record_event("bn_ws_depth", local_event, {"seq": 1})
    row = next(
        item for item in await monitor.snapshot() if item["source"] == "bn_ws_depth"
    )

    assert row["last_event_at"] == datetime(2026, 3, 22, 0, 0, tzinfo=timezone.utc)
    with pytest.raises(ValueError):
        await monitor.record_event(
            "bn_ws_depth", datetime(2026, 3, 22, 0, 0), {"seq": 2}
        )
