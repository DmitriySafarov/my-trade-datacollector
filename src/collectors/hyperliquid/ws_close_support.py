from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from .ws_session import HyperliquidWsSession

if TYPE_CHECKING:
    from .ws_manager import HyperliquidWsManager


async def close_session(
    session: HyperliquidWsSession, *, timeout_seconds: float
) -> None:
    try:
        await asyncio.wait_for(
            asyncio.shield(asyncio.create_task(asyncio.to_thread(session.close))),
            timeout=timeout_seconds,
        )
    except TimeoutError as error:
        raise RuntimeError("Hyperliquid websocket session close timed out") from error
    if session.is_alive():
        raise RuntimeError("Hyperliquid websocket thread did not stop")


async def close_manager_session(
    manager: HyperliquidWsManager,
    *,
    reason: str,
    timeout_seconds: float,
    logger: logging.Logger,
    retry_failed_session: bool = False,
) -> None:
    async with manager._session_lock:
        session = manager._session
        if session is None or (
            session is manager._failed_close_session and not retry_failed_session
        ):
            return
        close_task = manager._session_close_task
        should_retry_close = (
            retry_failed_session
            and session is manager._failed_close_session
            and _completed_close_needs_retry(session, close_task)
        )
        if close_task is None or (should_retry_close):
            close_task = asyncio.create_task(asyncio.to_thread(session.close))
            manager._session_close_task = close_task
    try:
        await asyncio.wait_for(asyncio.shield(close_task), timeout=timeout_seconds)
    except TimeoutError as error:
        async with manager._session_lock:
            if manager._session is session:
                manager._failed_close_session = session
        manager._bridge.last_error = repr(error)
        logger.warning(
            "hyperliquid_ws_session_close_failed reason=%s error=%r",
            reason,
            error,
        )
        raise RuntimeError("Hyperliquid websocket session close timed out") from error
    except Exception as error:
        async with manager._session_lock:
            if manager._session is session:
                manager._failed_close_session = session
        manager._bridge.last_error = repr(error)
        logger.warning(
            "hyperliquid_ws_session_close_failed reason=%s error=%r",
            reason,
            error,
        )
        raise
    if session.is_alive():
        error = RuntimeError("Hyperliquid websocket thread did not stop")
        async with manager._session_lock:
            if manager._session is session:
                manager._failed_close_session = session
        manager._bridge.last_error = repr(error)
        logger.warning(
            "hyperliquid_ws_session_close_failed reason=%s error=%r",
            reason,
            error,
        )
        raise error
    async with manager._session_lock:
        if manager._session is session:
            manager._session = None
        if manager._failed_close_session is session:
            manager._failed_close_session = None
        if manager._session_close_task is close_task:
            manager._session_close_task = None


def _completed_close_needs_retry(
    session: HyperliquidWsSession,
    close_task: asyncio.Task[None] | None,
) -> bool:
    if close_task is None or not close_task.done():
        return False
    if close_task.cancelled():
        return True
    return close_task.exception() is not None or session.is_alive()
