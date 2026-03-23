from __future__ import annotations

import asyncio

import pytest

from src.app import CollectorApplication, CollectorShutdownError, stop_application
from src.app_lifecycle import CollectorStartupError
from src.collectors.base import BaseCollector


class _StubCollector(BaseCollector):
    name = "stub"
    source_ids = ("stub_source",)

    def __init__(self) -> None:
        self.started = False
        self.stopped = False
        self.stop_calls = 0
        self.ready = asyncio.Event()
        self.release = asyncio.Event()

    async def start(self) -> None:
        self.started = True
        self.ready.set()
        await self.release.wait()

    async def stop(self) -> None:
        self.stop_calls += 1
        self.stopped = True
        self.release.set()

    async def wait_ready(self) -> None:
        await self.ready.wait()

    def health_snapshot(self) -> dict[str, object]:
        return {"name": self.name, "started": self.started}


class _StubCollectorApplication(CollectorApplication):
    def __init__(self, config, collectors: list[BaseCollector]) -> None:
        super().__init__(config)
        self._startup_collectors = collectors

    def _build_collectors(self) -> list[BaseCollector]:
        return self._startup_collectors


class _StopRequestingCollector(_StubCollector):
    def __init__(self, on_start) -> None:
        super().__init__()
        self._on_start = on_start

    async def start(self) -> None:
        self.started = True
        self.ready.set()
        self._on_start()
        await self.release.wait()


@pytest.mark.asyncio
async def test_start_fails_when_runtime_collector_factory_returns_no_collectors(
    monkeypatch: pytest.MonkeyPatch,
    migrated_db: dict[str, object],
) -> None:
    monkeypatch.setattr("src.app.build_runtime_collectors", lambda **_: [])
    app = CollectorApplication(migrated_db["config"])

    with pytest.raises(CollectorStartupError, match="produced no collectors"):
        await app.start()
    await stop_application(app)


@pytest.mark.asyncio
async def test_start_uses_runtime_collector_factory_when_configured(
    monkeypatch: pytest.MonkeyPatch,
    migrated_db: dict[str, object],
) -> None:
    collector = _StubCollector()
    monkeypatch.setattr("src.app.build_runtime_collectors", lambda **_: [collector])
    app = CollectorApplication(migrated_db["config"])

    await app.start()

    assert collector.started is True
    assert app.health_snapshot()["collectors"] == [collector.health_snapshot()]

    await stop_application(app)
    assert collector.stopped is True


@pytest.mark.asyncio
async def test_start_returns_while_long_lived_collectors_run(
    migrated_db: dict[str, object],
) -> None:
    collector = _StubCollector()
    app = _StubCollectorApplication(migrated_db["config"], [collector])

    await asyncio.wait_for(app.start(), timeout=0.1)

    assert collector.started is True
    await stop_application(app)
    assert collector.stopped is True


@pytest.mark.asyncio
async def test_start_short_circuits_remaining_collectors_after_stop_request(
    migrated_db: dict[str, object],
) -> None:
    app = _StubCollectorApplication(migrated_db["config"], [])
    first = _StopRequestingCollector(app.request_stop)
    second = _StubCollector()
    app._startup_collectors = [first, second]

    await app.start()

    assert first.started is True
    assert first.stopped is True
    assert first.stop_calls == 1
    assert second.started is False
    assert second.stop_calls == 0
    await stop_application(app)


@pytest.mark.asyncio
async def test_health_snapshot_reports_stopping_when_stop_requested(
    migrated_db: dict[str, object],
) -> None:
    collector = _StubCollector()
    app = _StubCollectorApplication(migrated_db["config"], [collector])

    await app.start()
    app.request_stop()

    assert app.health_snapshot()["status"] == "stopping"
    await stop_application(app)


class _NeverReadyCollector(_StubCollector):
    async def start(self) -> None:
        self.started = True
        await self.release.wait()


class _ExitingCollector(_StubCollector):
    name = "exiting"

    async def start(self) -> None:
        self.started = True
        self.ready.set()


@pytest.mark.asyncio
async def test_start_times_out_when_collector_never_signals_ready(
    migrated_db: dict[str, object],
) -> None:
    config = migrated_db["config"]
    app = _StubCollectorApplication(
        config.__class__(
            **(config.__dict__ | {"collector_startup_timeout_seconds": 0.01})
        ),
        [_NeverReadyCollector()],
    )

    with pytest.raises(CollectorStartupError, match="readiness timed out"):
        await app.start()

    await stop_application(app)


@pytest.mark.asyncio
async def test_run_stops_when_collector_task_exits(
    migrated_db: dict[str, object],
) -> None:
    collector = _ExitingCollector()
    app = _StubCollectorApplication(migrated_db["config"], [collector])

    await app.start()

    assert app.health_snapshot()["status"] == "degraded"
    with pytest.raises(
        CollectorShutdownError,
        match="collector task exited unexpectedly: exiting",
    ):
        await app.run()

    assert app.stop_event.is_set()
    await stop_application(app)
