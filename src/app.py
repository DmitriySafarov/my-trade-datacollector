from __future__ import annotations

import asyncio
import logging
import signal
from pathlib import Path
from time import monotonic

import asyncpg

from src.app_cleanup_state import cleanup_required_collectors
from src.app_collectors import start_collectors, stop_collectors
from src.app_logging import configure_logging
from src.app_lifecycle import (
    ApplicationStopTimeoutError,
    CollectorShutdownError,
    CollectorStartupError,
    stop_application,
)
from src.app_runtime import run_until_stopped
from src.collectors import build_runtime_collectors
from src.collectors.base import BaseCollector
from src.config.bootstrap import BootstrapConfig, load_bootstrap_config
from src.config.runtime import RuntimeSettings, load_runtime_settings
from src.db.migrations import run_migrations
from src.db.pool import close_pool, create_pool
from src.health.server import HealthServer

LOGGER = logging.getLogger(__name__)
__all__ = [
    "ApplicationStopTimeoutError",
    "CollectorApplication",
    "CollectorShutdownError",
    "CollectorStartupError",
    "run_application",
    "stop_application",
]


class CollectorApplication:
    def __init__(self, config: BootstrapConfig) -> None:
        self.config = config
        self.started_at = monotonic()
        self.stop_event = asyncio.Event()
        self.pool: asyncpg.Pool | None = None
        self.runtime_settings: RuntimeSettings | None = None
        self.collectors: list[BaseCollector] = []
        self.collector_tasks: dict[BaseCollector, asyncio.Task[None]] = {}
        self.health_server = HealthServer(
            host=config.health_host,
            port=config.health_port,
            snapshot_provider=self.health_snapshot,
        )

    async def start(self) -> None:
        configure_logging(self.config.log_level)
        self._install_signal_handlers()
        self.pool = await create_pool(self.config)
        if self.stop_event.is_set():
            return
        applied = await run_migrations(
            self.pool,
            Path("migrations"),
            stop_requested=self.stop_event.is_set,
        )
        if self.stop_event.is_set():
            return
        self.runtime_settings = await load_runtime_settings(self.pool, self.config)
        if self.stop_event.is_set():
            return
        configured_collectors = self._build_collectors()
        if not configured_collectors:
            raise CollectorStartupError("collector startup produced no collectors")
        try:
            started_collectors = await start_collectors(
                configured_collectors,
                self.collector_tasks,
                stop_event=self.stop_event,
                startup_timeout=self.config.collector_startup_timeout_seconds,
            )
        except BaseException as error:
            self.collectors = cleanup_required_collectors(error)
            raise
        self.collectors = started_collectors
        if self.stop_event.is_set():
            return
        await self.health_server.start()
        LOGGER.info("collector_started applied_migrations=%s", applied)

    async def run(self) -> None:
        await run_until_stopped(self.stop_event, self.collector_tasks)

    async def stop(self) -> None:
        failures: list[tuple[str, BaseException]] = []
        collector_failures = await stop_collectors(
            self.collectors, self.collector_tasks
        )
        failures.extend(
            (collector.name, error) for collector, error in collector_failures
        )
        try:
            await self.health_server.stop()
        except Exception as error:
            LOGGER.error("health_server_stop_failed", exc_info=error)
            failures.append(("health_server", error))
        pool = self.pool
        self.pool = None
        if pool is not None:
            try:
                await close_pool(pool)
            except Exception as error:
                LOGGER.error("pool_close_failed", exc_info=error)
                failures.append(("db_pool", error))
        if not failures:
            self.collectors = []
        if failures:
            failed_components = ", ".join(name for name, _ in failures)
            raise CollectorShutdownError(
                f"collector shutdown failed for: {failed_components}"
            )
        LOGGER.info("collector_stopped")

    def request_stop(self) -> None:
        self.stop_event.set()

    def health_snapshot(self) -> dict[str, object]:
        missing_tasks = len(self.collector_tasks) != len(self.collectors)
        unhealthy_tasks = any(task.done() for task in self.collector_tasks.values())
        if self.stop_event.is_set():
            status = "stopping"
        elif not self.collectors or missing_tasks or unhealthy_tasks:
            status = "degraded"
        else:
            status = "ok"
        return {
            "status": status,
            "service": self.config.service_name,
            "uptime_seconds": round(monotonic() - self.started_at, 3),
            "db_connected": self.pool is not None,
            "runtime_settings": self.runtime_settings.__dict__
            if self.runtime_settings
            else {},
            "collectors": [
                collector.health_snapshot() for collector in self.collectors
            ],
        }

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for signame in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(signame, self.request_stop)

    def _build_collectors(self) -> list[BaseCollector]:
        if self.collectors:
            return list(self.collectors)
        return build_runtime_collectors(
            config=self.config,
            pool=self.pool,
            runtime_settings=self.runtime_settings,
        )


async def run_application() -> None:
    app = CollectorApplication(load_bootstrap_config())
    primary_error: BaseException | None = None
    try:
        await app.start()
        await app.run()
    except BaseException as error:
        primary_error = error
        raise
    finally:
        try:
            await stop_application(app)
        except Exception as stop_error:
            if primary_error is None:
                raise
            primary_error.add_note(
                f"Shutdown also failed during cleanup: {stop_error!r}"
            )
            LOGGER.error("collector_stop_after_failure_failed", exc_info=stop_error)
