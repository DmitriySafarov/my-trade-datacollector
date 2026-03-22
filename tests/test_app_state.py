from __future__ import annotations

import asyncio

import pytest

from src.app import CollectorApplication, stop_application
from src.app_lifecycle import CollectorStartupError
from tests._db_test_support import _test_config


class _NeverReadyCollector:
    name = "never-ready"
    source_ids = ("never_ready_source",)

    def __init__(self) -> None:
        self.stop_calls = 0

    async def start(self) -> None:
        await asyncio.Event().wait()

    async def wait_ready(self) -> None:
        await asyncio.Event().wait()

    async def stop(self) -> None:
        self.stop_calls += 1

    def health_snapshot(self) -> dict[str, object]:
        return {"name": self.name}


@pytest.mark.asyncio
async def test_startup_timeout_does_not_preserve_rolled_back_collectors(
    migrated_db: dict[str, object],
) -> None:
    collector = _NeverReadyCollector()
    config = migrated_db["config"]

    class _TimeoutStartupApp(CollectorApplication):
        def _build_collectors(self):
            return [collector]

    app = _TimeoutStartupApp(
        config.__class__(
            **(config.__dict__ | {"collector_startup_timeout_seconds": 0.01})
        )
    )

    with pytest.raises(CollectorStartupError, match="readiness timed out"):
        await app.start()

    assert collector.stop_calls == 1
    assert app.collectors == []
    assert app.health_snapshot()["status"] == "degraded"

    await stop_application(app)
    assert collector.stop_calls == 1


def test_health_snapshot_degrades_when_collectors_have_no_tasks() -> None:
    app = CollectorApplication(_test_config("collector"))
    app.collectors = [_NeverReadyCollector()]

    assert app.health_snapshot()["status"] == "degraded"


@pytest.mark.asyncio
async def test_startup_failure_preserves_only_collectors_with_failed_cleanup(
    migrated_db: dict[str, object],
) -> None:
    class _ReadyCollector(_NeverReadyCollector):
        def __init__(self) -> None:
            super().__init__()
            self.ready = asyncio.Event()
            self.release = asyncio.Event()

        async def start(self) -> None:
            self.ready.set()
            await self.release.wait()

        async def wait_ready(self) -> None:
            await self.ready.wait()

        async def stop(self) -> None:
            self.stop_calls += 1
            self.release.set()

    class _CleanupFailingCollector(_ReadyCollector):
        async def stop(self) -> None:
            self.stop_calls += 1
            self.release.set()
            raise RuntimeError("cleanup failed")

    class _StartFailingCollector(_NeverReadyCollector):
        async def start(self) -> None:
            raise RuntimeError("startup failed")

        async def stop(self) -> None:
            self.stop_calls += 1
            raise RuntimeError("cleanup failed")

    survivor = _ReadyCollector()
    cleanup_failed = _CleanupFailingCollector()
    startup_failed = _StartFailingCollector()

    class _FailingStartupApp(CollectorApplication):
        def _build_collectors(self):
            return [survivor, cleanup_failed, startup_failed]

    app = _FailingStartupApp(migrated_db["config"])

    with pytest.raises(RuntimeError, match="startup failed"):
        await app.start()

    assert survivor.stop_calls == 1
    assert cleanup_failed.stop_calls == 1
    assert startup_failed.stop_calls == 1
    assert app.collectors == [cleanup_failed, startup_failed]
