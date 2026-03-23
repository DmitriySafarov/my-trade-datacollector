from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .ws_manager import HyperliquidWsManager


def begin_run(manager: HyperliquidWsManager) -> None:
    if not manager._run_stopped.is_set():
        raise RuntimeError("Hyperliquid websocket manager is already running")
    manager._run_stopped.clear()
    manager._reconnect_count = 0


def finish_run(manager: HyperliquidWsManager) -> None:
    manager._stop_requested = False
    manager._stop_event.clear()
    manager._run_stopped.set()
