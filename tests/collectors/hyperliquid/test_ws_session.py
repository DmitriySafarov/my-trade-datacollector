from __future__ import annotations

from types import SimpleNamespace

from src.collectors.hyperliquid.ws_session import SdkHyperliquidWsSession


class _FakeWebsocketManager:
    def __init__(self) -> None:
        self.ident = None
        self._alive = False
        self.start_calls = 0
        self.stop_calls = 0
        self.join_calls = 0
        self.subscriptions: list[tuple[object, object]] = []
        self.events: list[str] = []
        self.ws = SimpleNamespace(
            on_open=self._original_on_open,
            on_close=lambda _ws, _status_code, _message: None,
            on_error=lambda _ws, _error: None,
        )

    def _original_on_open(self, _ws) -> None:
        self.events.append("original_open")

    def start(self) -> None:
        self.start_calls += 1
        self.ident = 1
        self._alive = True

    def subscribe(self, subscription: object, callback: object) -> None:
        self.subscriptions.append((subscription, callback))

    def stop(self) -> None:
        self.stop_calls += 1
        self._alive = False

    def join(self, timeout: float) -> None:
        del timeout
        self.join_calls += 1

    def is_alive(self) -> bool:
        return self._alive


def _build_session(monkeypatch, manager: _FakeWebsocketManager, **callbacks):
    monkeypatch.setattr(
        "src.collectors.hyperliquid.ws_session.WebsocketManager",
        lambda _base_url: manager,
    )
    return SdkHyperliquidWsSession("https://api.hyperliquid.xyz", **callbacks)


def test_sdk_session_wraps_open_callback(monkeypatch) -> None:
    manager = _FakeWebsocketManager()
    events: list[str] = []
    _build_session(
        monkeypatch,
        manager,
        on_open=lambda: events.append("session_open"),
        on_close=lambda _reason: None,
        on_error=lambda _error: None,
    )

    manager.ws.on_open(object())

    assert manager.events == ["original_open"]
    assert events == ["session_open"]


def test_sdk_session_wraps_close_callback(monkeypatch) -> None:
    manager = _FakeWebsocketManager()
    reasons: list[str | None] = []
    _build_session(
        monkeypatch,
        manager,
        on_open=lambda: None,
        on_close=reasons.append,
        on_error=lambda _error: None,
    )

    manager.ws.on_close(object(), 1000, "bye")
    manager.ws.on_close(object(), 1000, None)

    assert reasons == ["bye", None]


def test_sdk_session_wraps_error_callback(monkeypatch) -> None:
    manager = _FakeWebsocketManager()
    errors: list[BaseException] = []
    _build_session(
        monkeypatch,
        manager,
        on_open=lambda: None,
        on_close=lambda _reason: None,
        on_error=errors.append,
    )

    manager.ws.on_error(object(), "boom")
    manager.ws.on_error(object(), RuntimeError("kapow"))

    assert isinstance(errors[0], RuntimeError)
    assert str(errors[0]) == "boom"
    assert isinstance(errors[1], RuntimeError)
    assert str(errors[1]) == "kapow"


def test_sdk_session_subscribe_and_start_delegate_to_manager(monkeypatch) -> None:
    manager = _FakeWebsocketManager()
    session = _build_session(
        monkeypatch,
        manager,
        on_open=lambda: None,
        on_close=lambda _reason: None,
        on_error=lambda _error: None,
    )
    subscription = {"type": "trades", "coin": "ETH"}

    def callback(message: object) -> object:
        return message

    session.subscribe(subscription, callback)
    session.start()

    assert manager.subscriptions == [(subscription, callback)]
    assert manager.start_calls == 1
    assert session.is_alive() is True


def test_sdk_session_close_is_safe_before_thread_start(monkeypatch) -> None:
    manager = _FakeWebsocketManager()
    session = _build_session(
        monkeypatch,
        manager,
        on_open=lambda: None,
        on_close=lambda _reason: None,
        on_error=lambda _error: None,
    )

    session.close()

    assert manager.stop_calls == 1
    assert manager.join_calls == 0


def test_sdk_session_close_joins_started_thread(monkeypatch) -> None:
    manager = _FakeWebsocketManager()
    session = _build_session(
        monkeypatch,
        manager,
        on_open=lambda: None,
        on_close=lambda _reason: None,
        on_error=lambda _error: None,
    )

    session.start()
    session.close()

    assert manager.stop_calls == 1
    assert manager.join_calls == 1
    assert session.is_alive() is False
