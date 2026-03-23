from __future__ import annotations

from collections.abc import Callable
from typing import Protocol

from hyperliquid.utils.types import Subscription, WsMsg
from hyperliquid.websocket_manager import WebsocketManager


OpenCallback = Callable[[], None]
CloseCallback = Callable[[str | None], None]
ErrorCallback = Callable[[BaseException], None]
MessageCallback = Callable[[WsMsg], None]


class HyperliquidWsSession(Protocol):
    def start(self) -> None: ...

    def subscribe(
        self, subscription: Subscription, callback: MessageCallback
    ) -> None: ...

    def close(self) -> None: ...

    def is_alive(self) -> bool: ...


SessionFactory = Callable[
    [str, OpenCallback, CloseCallback, ErrorCallback],
    HyperliquidWsSession,
]


class SdkHyperliquidWsSession:
    def __init__(
        self,
        base_url: str,
        on_open: OpenCallback,
        on_close: CloseCallback,
        on_error: ErrorCallback,
    ) -> None:
        self._manager = WebsocketManager(base_url)
        original_on_open = self._manager.ws.on_open

        def wrapped_on_open(ws) -> None:
            original_on_open(ws)
            on_open()

        def wrapped_on_close(_ws, _status_code, message) -> None:
            on_close(str(message) if message is not None else None)

        def wrapped_on_error(_ws, error) -> None:
            exception = (
                error if isinstance(error, BaseException) else RuntimeError(str(error))
            )
            on_error(exception)

        self._manager.ws.on_open = wrapped_on_open
        self._manager.ws.on_close = wrapped_on_close
        self._manager.ws.on_error = wrapped_on_error

    def start(self) -> None:
        self._manager.start()

    def subscribe(self, subscription: Subscription, callback: MessageCallback) -> None:
        self._manager.subscribe(subscription, callback)

    def close(self) -> None:
        started = self._manager.ident is not None
        self._manager.stop()
        if not started:
            return
        self._manager.join(timeout=5.0)
        if self._manager.is_alive():
            raise RuntimeError("Hyperliquid websocket thread did not stop")

    def is_alive(self) -> bool:
        return self._manager.is_alive()
