from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine, Sequence

from . import ws_manager_support as support, ws_run_control
from .ws_bridge import HyperliquidWsBridge
from .ws_close_support import close_manager_session
from .lifecycle_support import merge_error
from .ws_session import HyperliquidWsSession, SessionFactory, SdkHyperliquidWsSession
from .ws_subscription import HyperliquidWsSubscription

LOGGER = logging.getLogger(__name__)
SESSION_CLOSE_TIMEOUT_SECONDS = 6.0
wait_for_stop_or_timeout = support.wait_for_stop_or_timeout


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
        self._stop_requested = False
        self._run_stopped = asyncio.Event()
        self._run_stopped.set()
        self._session: HyperliquidWsSession | None = None
        self._failed_close_session: HyperliquidWsSession | None = None
        self._session_close_task: asyncio.Task[None] | None = None
        self._session_lock = asyncio.Lock()
        self._reconnect_count = 0
        self._bridge = HyperliquidWsBridge(
            subscriptions=subscriptions,
            queue_maxsize=queue_maxsize,
            on_fatal=self._stop_event.set,
        )

    def run(self) -> Coroutine[object, object, None]:
        ws_run_control.begin_run(self)
        return self._run()

    async def _run(self) -> None:
        error: BaseException | None = None
        bridge_drained = False
        try:
            if self._stop_requested:
                return
            self._stop_event.clear()
            await self._bridge.start()
            await self._supervise()
            if self._bridge.failure is None:
                timeout = (
                    self.shutdown_drain_timeout_seconds
                    if self._stop_event.is_set()
                    else None
                )
                await support.drain_bridge(
                    self._bridge,
                    timeout=timeout,
                    logger=LOGGER if timeout is not None else None,
                )
                bridge_drained = True
            if self._bridge.failure is not None:
                raise self._bridge.failure
        except BaseException as caught:
            error = caught
        finally:
            try:
                await close_manager_session(
                    self,
                    reason="shutdown_cleanup",
                    timeout_seconds=SESSION_CLOSE_TIMEOUT_SECONDS,
                    logger=LOGGER,
                    retry_failed_session=True,
                )
            except BaseException as close_error:
                error = merge_error(error, close_error)
            try:
                await support.drain_after_close(self, bridge_drained=bridge_drained)
            except BaseException as drain_error:
                error = merge_error(error, drain_error)
            finally:
                await self._bridge.stop()
                ws_run_control.finish_run(self)
        if error is not None:
            raise error

    async def stop(self) -> None:
        if self._run_stopped.is_set():
            return
        self._stop_requested = True
        self._stop_event.set()
        self._bridge.begin_shutdown()
        timeout = (
            self.shutdown_drain_timeout_seconds + 2 * SESSION_CLOSE_TIMEOUT_SECONDS
        )
        try:
            await asyncio.wait_for(self._run_stopped.wait(), timeout=timeout)
        except TimeoutError as error:
            raise RuntimeError("Hyperliquid websocket stop timed out") from error

    async def wait_ready(self) -> None:
        await support.wait_bridge_ready(self._bridge)

    def health_snapshot(self):
        return support.build_bridge_health_snapshot(self._bridge, self._reconnect_count)

    async def _supervise(self) -> None:
        attempt = 0
        recovering = False
        while not self._stop_event.is_set() and self._bridge.failure is None:
            generation = self._bridge.next_generation()
            opened, closed, errored = asyncio.Event(), asyncio.Event(), asyncio.Event()
            self._bridge.state = "reconnecting" if recovering else "connecting"
            try:
                session = support.open_session(
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
                    self._failed_close_session = self._session_close_task = None
                session.start()
            except Exception as error:
                await support.cleanup_setup_failure(
                    self,
                    generation=generation,
                    error=error,
                    logger=LOGGER,
                    timeout_seconds=SESSION_CLOSE_TIMEOUT_SECONDS,
                )
            else:
                await support.wait_for_disconnect(
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
                await close_manager_session(
                    self,
                    reason="reconnect_cleanup",
                    timeout_seconds=SESSION_CLOSE_TIMEOUT_SECONDS,
                    logger=LOGGER,
                )
            if self._stop_event.is_set() or self._bridge.failure is not None:
                break
            recovering = True
            self._bridge.state = "reconnecting"
            self._reconnect_count += 1
            attempt = 0 if opened.is_set() else attempt
            delay = support.compute_reconnect_delay(
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
