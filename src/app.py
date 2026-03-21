from __future__ import annotations

import asyncio
import logging
import signal
from pathlib import Path
from time import monotonic

import asyncpg

from src.app_logging import configure_logging
from src.collectors.base import BaseCollector
from src.config.bootstrap import BootstrapConfig, load_bootstrap_config
from src.config.runtime import RuntimeSettings, load_runtime_settings
from src.db.migrations import run_migrations
from src.db.pool import create_pool
from src.health.server import HealthServer


LOGGER = logging.getLogger(__name__)


class CollectorApplication:
    def __init__(self, config: BootstrapConfig) -> None:
        self.config = config
        self.started_at = monotonic()
        self.stop_event = asyncio.Event()
        self.pool: asyncpg.Pool | None = None
        self.runtime_settings: RuntimeSettings | None = None
        self.collectors: list[BaseCollector] = []
        self.health_server = HealthServer(
            host=config.health_host,
            port=config.health_port,
            snapshot_provider=self.health_snapshot,
        )

    async def start(self) -> None:
        configure_logging(self.config.log_level)
        self._install_signal_handlers()
        self.pool = await create_pool(self.config)
        applied = await run_migrations(self.pool, Path("migrations"))
        self.runtime_settings = await load_runtime_settings(self.pool, self.config)
        await self.health_server.start()
        LOGGER.info("collector_started applied_migrations=%s", applied)

    async def run(self) -> None:
        await self.stop_event.wait()

    async def stop(self) -> None:
        stop_tasks = [collector.stop() for collector in self.collectors]
        if stop_tasks:
            await asyncio.gather(*stop_tasks, return_exceptions=True)

        await self.health_server.stop()
        if self.pool is not None:
            await self.pool.close()
            self.pool = None
        LOGGER.info("collector_stopped")

    def request_stop(self) -> None:
        self.stop_event.set()

    def health_snapshot(self) -> dict[str, object]:
        return {
            "status": "ok",
            "service": self.config.service_name,
            "uptime_seconds": round(monotonic() - self.started_at, 3),
            "db_connected": self.pool is not None,
            "runtime_settings": (
                self.runtime_settings.__dict__ if self.runtime_settings else {}
            ),
            "collectors": [collector.health_snapshot() for collector in self.collectors],
        }

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for signame in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(signame, self.request_stop)


async def run_application() -> None:
    app = CollectorApplication(load_bootstrap_config())
    await app.start()
    try:
        await app.run()
    finally:
        await asyncio.wait_for(
            app.stop(),
            timeout=app.config.shutdown_timeout_seconds,
        )
