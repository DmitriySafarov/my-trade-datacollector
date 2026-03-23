from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence

from .ws_bridge import HyperliquidWsBridge
from .ws_manager_support import (
    build_bridge_health_snapshot,
    compute_reconnect_delay,
    handle_session_error,
    wait_for_disconnect,
    wait_bridge_ready,
    wait_for_stop_or_timeout,
)
from .ws_session import (
    HyperliquidWsSession,
    SessionFactory,
    SdkHyperliquidWsSession,
)
from .ws_subscription import HyperliquidWsSubscription


LOGGER = logging.getLogger(__name__)


class HyperliquidWsManager:
    def __init__(
        self,
        *,
        base_url: str,
        subscriptions: Sequence[HyperliquidWsSubscription],
        reconnect_base_seconds: float = 1.0,
        reconnect_max_seconds: float = 30.0,
        reconnect_jitter_ratio: float = 0.2,
        open_timeout_seconds: float = 10.0,
        shutdown_drain_timeout_seconds: float = 5.0,
        queue_maxsize: int = 10_000,
        session_factory: SessionFactory = SdkHyperliquidWsSession,
    ) -> None:
        self.base_url = base_url
        self.reconnect_base_seconds = reconnect_base_seconds
        self.reconnect_max_seconds = reconnect_max_seconds
        self.reconnect_jitter_ratio = reconnect_jitter_ratio
        self.open_timeout_seconds = open_timeout_seconds
        self.shutdown_drain_timeout_seconds = shutdown_drain_timeout_seconds
        self._session_factory = session_factory
        self._stop_event = asyncio.Event()
        self._run_stopped = asyncio.Event()
        self._run_stopped.set()
        self._session: HyperliquidWsSession | None = None
        self._session_lock = asyncio.Lock()
        self._reconnect_count = 0
        self._bridge = HyperliquidWsBridge(
            subscriptions=subscriptions,
            queue_maxsize=queue_maxsize,
            on_fatal=self._stop_event.set,
        )

    async def run(self) -> None:
        self._run_stopped.clear()
        await self._bridge.start()
        try:
            await self._supervise()
            if self._bridge.failure is None:
                drained = await self._bridge.drain(
                    timeout=(
                        self.shutdown_drain_timeout_seconds
                        if self._stop_event.is_set()
                        else None
                    )
                )
                if not drained and self._stop_event.is_set():
                    LOGGER.warning(
                        "hyperliquid_ws_shutdown_drain_timeout timeout_seconds=%.2f",
                        self.shutdown_drain_timeout_seconds,
                    )
            if self._bridge.failure is not None:
                raise self._bridge.failure
        finally:
            await self._close_session()
            await self._bridge.stop()
            self._run_stopped.set()

    async def stop(self) -> None:
        self._stop_event.set()
        self._bridge.begin_shutdown()
        await self._close_session()
        if not self._run_stopped.is_set():
            await self._run_stopped.wait()

    async def wait_ready(self) -> None:
        await wait_bridge_ready(self._bridge)

    def health_snapshot(self) -> dict[str, object]:
        return build_bridge_health_snapshot(self._bridge, self._reconnect_count)

    async def _supervise(self) -> None:
        attempt = 0
        recovering = False
        while not self._stop_event.is_set() and self._bridge.failure is None:
            generation = self._bridge.next_generation()
            opened = asyncio.Event()
            closed = asyncio.Event()
            errored = asyncio.Event()
            self._bridge.state = "reconnecting" if recovering else "connecting"
            try:
                session = await self._open_session(generation, opened, closed, errored)
            except Exception as error:
                await self._handle_setup_error(generation, error)
            else:
                await wait_for_disconnect(
                    session=session,
                    opened=opened,
                    closed=closed,
                    errored=errored,
                    reconnect_requested=self._bridge.reconnect_requested,
                    stop_event=self._stop_event,
                    has_failure=lambda: self._bridge.failure is not None,
                    open_timeout_seconds=self.open_timeout_seconds,
                    on_open_timeout=lambda: self._bridge.request_reconnect(
                        generation=generation,
                        reason="open_timeout",
                    ),
                    on_session_exit=lambda: self._bridge.request_reconnect(
                        generation=generation,
                        reason="session_exited",
                    ),
                )
                await self._close_session()
            if self._stop_event.is_set() or self._bridge.failure is not None:
                break
            recovering = True
            self._bridge.state = "reconnecting"
            self._reconnect_count += 1
            if opened.is_set():
                attempt = 0
            delay = compute_reconnect_delay(
                base_seconds=self.reconnect_base_seconds,
                max_seconds=self.reconnect_max_seconds,
                attempt=attempt,
                jitter_ratio=self.reconnect_jitter_ratio,
            )
            if not opened.is_set():
                attempt += 1
            LOGGER.warning(
                "hyperliquid_ws_reconnecting reason=%s delay_seconds=%.2f",
                self._bridge.last_disconnect_reason,
                delay,
            )
            if await wait_for_stop_or_timeout(self._stop_event, delay):
                break

    async def _open_session(
        self,
        generation: int,
        opened: asyncio.Event,
        closed: asyncio.Event,
        errored: asyncio.Event,
    ) -> HyperliquidWsSession:
        session = self._session_factory(
            self.base_url,
            self._bridge.threadsafe_callback(
                self._bridge.handle_open, generation, opened
            ),
            self._bridge.threadsafe_callback(
                self._bridge.handle_close,
                generation,
                closed,
            ),
            self._bridge.threadsafe_callback(
                handle_session_error,
                self._bridge,
                generation,
                errored,
            ),
        )
        async with self._session_lock:
            self._session = session
        for key, definition in self._bridge.subscriptions.items():
            session.subscribe(
                definition.subscription,
                self._bridge.message_callback(generation, key),
            )
        session.start()
        return session

    async def _close_session(self) -> None:
        async with self._session_lock:
            session = self._session
            self._session = None
        if session is not None:
            await asyncio.to_thread(session.close)

    async def _handle_setup_error(self, generation: int, error: BaseException) -> None:
        self._bridge.handle_error(generation, error)
        self._bridge.last_disconnect_reason = f"setup_failed:{type(error).__name__}"
        await self._close_session()
