from __future__ import annotations

from collections.abc import Callable
from typing import Any

from aiohttp import web


class HealthServer:
    def __init__(
        self,
        host: str,
        port: int,
        snapshot_provider: Callable[[], dict[str, Any]],
    ) -> None:
        self.host = host
        self.port = port
        self.snapshot_provider = snapshot_provider
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None

    async def start(self) -> None:
        app = web.Application()
        app.router.add_get("/health", self._handle_health)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, self.host, self.port)
        await self._site.start()

    async def stop(self) -> None:
        if self._runner is None:
            return
        await self._runner.cleanup()
        self._runner = None
        self._site = None

    async def _handle_health(self, _: web.Request) -> web.Response:
        return web.json_response(self.snapshot_provider())
