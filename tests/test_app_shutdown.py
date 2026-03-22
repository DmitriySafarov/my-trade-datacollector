from __future__ import annotations

import asyncio
from time import monotonic
from types import SimpleNamespace

import pytest

from src.app import (
    ApplicationStopTimeoutError,
    CollectorApplication,
    CollectorShutdownError,
    run_application,
    stop_application,
)
from tests._db_test_support import _test_config


class _FakeHealthServer:
    async def stop(self) -> None:
        return None


class _FailingCollector:
    name = "failing"
    source_ids = ("failing_source",)

    def __init__(self) -> None:
        self.stop_called = False
        self.ready = asyncio.Event()

    async def start(self) -> None:
        self.ready.set()

    async def wait_ready(self) -> None:
        await self.ready.wait()

    async def stop(self) -> None:
        self.stop_called = True
        raise RuntimeError("flush failed")

    def health_snapshot(self) -> dict[str, object]:
        return {"name": self.name}


class _StartFailingCollector(_FailingCollector):
    async def start(self) -> None:
        raise RuntimeError("startup failed")


class _SlowStopApp:
    def __init__(self) -> None:
        self.config = SimpleNamespace(shutdown_timeout_seconds=0.02)
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.finished = False

    async def stop(self) -> None:
        self.started.set()
        await self.release.wait()
        self.finished = True


class _CancellationResistantStopApp(_SlowStopApp):
    async def stop(self) -> None:
        self.started.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            await asyncio.Event().wait()


@pytest.mark.asyncio
async def test_stop_application_does_not_cancel_cleanup_on_timeout() -> None:
    app = _SlowStopApp()

    stop_task = asyncio.create_task(stop_application(app))
    await app.started.wait()
    await asyncio.sleep(0.025)

    assert not stop_task.done()

    app.release.set()
    await stop_task

    assert app.finished


@pytest.mark.asyncio
async def test_stop_application_raises_after_grace_timeout() -> None:
    app = _CancellationResistantStopApp()
    started_at = monotonic()

    with pytest.raises(ApplicationStopTimeoutError):
        await stop_application(app)

    assert app.started.is_set()
    assert not app.finished
    assert monotonic() - started_at < 0.2


@pytest.mark.asyncio
async def test_collector_application_stop_propagates_collector_failures(
    migrated_db: dict[str, object],
) -> None:
    app = CollectorApplication(_test_config("collector"))
    collector = _FailingCollector()
    app.collectors = [collector]
    app.pool = migrated_db["pool"]
    app.health_server = _FakeHealthServer()

    with pytest.raises(CollectorShutdownError):
        await app.stop()

    assert collector.stop_called is True
    assert app.collectors == [collector]
    assert app.pool is None


@pytest.mark.asyncio
async def test_startup_rollback_preserves_collectors_when_cleanup_fails(
    migrated_db: dict[str, object],
) -> None:
    survivor = _FailingCollector()
    failing = _StartFailingCollector()

    class _FailingStartupApp(CollectorApplication):
        def _build_collectors(self):
            return [survivor, failing]

    app = _FailingStartupApp(migrated_db["config"])

    with pytest.raises(RuntimeError, match="startup failed"):
        await app.start()

    assert survivor.stop_called is True
    assert failing.stop_called is True
    assert app.collectors == [survivor, failing]


@pytest.mark.asyncio
async def test_run_application_preserves_start_failure_when_cleanup_also_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fail_start(self) -> None:
        raise RuntimeError("startup failed")

    async def _fail_cleanup(app) -> None:
        raise CollectorShutdownError("cleanup failed")

    monkeypatch.setattr(
        "src.app.load_bootstrap_config", lambda: _test_config("collector")
    )
    monkeypatch.setattr("src.app.CollectorApplication.start", _fail_start)
    monkeypatch.setattr("src.app.stop_application", _fail_cleanup)

    with pytest.raises(RuntimeError, match="startup failed") as exc_info:
        await run_application()

    assert any(
        "cleanup failed" in note for note in getattr(exc_info.value, "__notes__", [])
    )
