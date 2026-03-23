from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence

from .ws_bridge import HyperliquidWsBridge
from .ws_manager_support import (
    build_bridge_health_snapshot,
    close_session,
    compute_reconnect_delay,
    open_session,
    wait_for_disconnect,
    wait_bridge_ready,
    wait_for_stop_or_timeout,
)
from .ws_session import HyperliquidWsSession, SessionFactory, SdkHyperliquidWsSession
from .ws_subscription import HyperliquidWsSubscription

LOGGER = logging.getLogger(__name__)
SESSION_CLOSE_TIMEOUT_SECONDS = 6.0


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
        error: BaseException | None = None
        try:
            await self._supervise()
            if self._bridge.failure is None:
                timeout = (
                    self.shutdown_drain_timeout_seconds
                    if self._stop_event.is_set()
                    else None
                )
                drained = await self._bridge.drain(timeout=timeout)
                if timeout is not None and not drained:
                    LOGGER.warning(
                        "hyperliquid_ws_shutdown_drain_timeout timeout_seconds=%.2f",
                        timeout,
                    )
            if self._bridge.failure is not None:
                raise self._bridge.failure
        except BaseException as caught:
            error = caught
        finally:
            try:
                if error is None:
                    await self._close_session(reason="shutdown_cleanup")
            except BaseException as close_error:
                error = close_error
            finally:
                await self._bridge.stop()
                self._run_stopped.set()
        if error is not None:
            raise error

    async def stop(self) -> None:
        self._stop_event.set()
        self._bridge.begin_shutdown()
        if not self._run_stopped.is_set():
            timeout = (
                self.shutdown_drain_timeout_seconds + SESSION_CLOSE_TIMEOUT_SECONDS
            )
            try:
                await asyncio.wait_for(self._run_stopped.wait(), timeout=timeout)
            except TimeoutError as error:
                raise RuntimeError("Hyperliquid websocket stop timed out") from error

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
                session = open_session(
                    session_factory=self._session_factory,
                    base_url=self.base_url,
                    bridge=self._bridge,
                    generation=generation,
                    opened=opened,
                    closed=closed,
                    errored=errored,
                )
                async with self._session_lock:
                    self._session = session
                session.start()
            except Exception as error:
                self._bridge.handle_error(generation, error)
                self._bridge.last_disconnect_reason = (
                    f"setup_failed:{type(error).__name__}"
                )
                await self._close_session(reason="setup_cleanup")
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
                await self._close_session(reason="reconnect_cleanup")
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

    async def _close_session(self, *, reason: str) -> None:
        async with self._session_lock:
            session = self._session
        if session is None:
            return
        try:
            await close_session(session, timeout_seconds=SESSION_CLOSE_TIMEOUT_SECONDS)
        except Exception as error:
            self._bridge.last_disconnect_reason = "session_close_timeout"
            self._bridge.last_error = repr(error)
            LOGGER.warning(
                "hyperliquid_ws_session_close_failed reason=%s error=%r",
                reason,
                error,
            )
            raise
        async with self._session_lock:
            if self._session is session:
                self._session = None
