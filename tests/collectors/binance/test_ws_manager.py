"""Tests for Binance combined stream WebSocket manager."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from unittest.mock import AsyncMock, patch

import aiohttp
import pytest

from src.collectors.binance.ws_manager import BinanceWsManager, DEFAULT_WS_URL
from src.collectors.binance.ws_receive_support import (
    MessageStats,
    handle_text,
    receive_loop,
)
from src.collectors.binance.ws_types import StreamConfig


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


@dataclass
class _Msg:
    """Minimal mock of ``aiohttp.WSMessage``."""

    type: aiohttp.WSMsgType
    data: str | int | None = None


def _stream_msg(stream: str, data: dict[str, Any]) -> _Msg:
    """Create a combined stream TEXT message."""
    return _Msg(aiohttp.WSMsgType.TEXT, json.dumps({"stream": stream, "data": data}))


class _FakeWs:
    """Minimal mock of ``aiohttp.ClientWebSocketResponse``."""

    def __init__(self, msgs: list[_Msg]) -> None:
        self._msgs = list(msgs)
        self._idx = 0
        self.closed = False

    def __aiter__(self) -> _FakeWs:
        return self

    async def __anext__(self) -> _Msg:
        if self._idx >= len(self._msgs):
            raise StopAsyncIteration
        msg = self._msgs[self._idx]
        self._idx += 1
        return msg

    async def close(self) -> None:
        self.closed = True


async def _noop_handler(data: Mapping[str, object]) -> None:
    """No-op stream handler for tests that don't inspect data."""


def _mock_session_ctx(ws_connect_rv: object) -> AsyncMock:
    """Build a mock ``aiohttp.ClientSession`` context manager.

    ``ws_connect_rv`` may be:
    - A callable (async function or AsyncMock with side_effect) → assigned directly
    - A non-callable (e.g. ``_FakeWs``) → wrapped in ``AsyncMock(return_value=...)``
    """
    session = AsyncMock()
    if callable(ws_connect_rv):
        session.ws_connect = ws_connect_rv
    else:
        session.ws_connect = AsyncMock(return_value=ws_connect_rv)
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=session)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return ctx


# ---------------------------------------------------------------------------
# Constructor & property tests
# ---------------------------------------------------------------------------


class TestInit:
    def test_no_streams_raises(self) -> None:
        with pytest.raises(ValueError, match="at least one"):
            BinanceWsManager(streams=[])

    def test_stream_count(self) -> None:
        mgr = BinanceWsManager(
            streams=[
                StreamConfig("a@b", _noop_handler),
                StreamConfig("c@d", _noop_handler),
            ]
        )
        assert mgr.stream_count == 2


class TestUrlBuilding:
    def test_single_stream(self) -> None:
        mgr = BinanceWsManager(
            streams=[StreamConfig("ethusdt@aggTrade", _noop_handler)]
        )
        assert mgr.build_url() == f"{DEFAULT_WS_URL}?streams=ethusdt@aggTrade"

    def test_multiple_streams_joined_with_slash(self) -> None:
        mgr = BinanceWsManager(
            streams=[
                StreamConfig("ethusdt@aggTrade", _noop_handler),
                StreamConfig("btcusdt@aggTrade", _noop_handler),
            ]
        )
        assert "ethusdt@aggTrade/btcusdt@aggTrade" in mgr.build_url()

    def test_trailing_slash_stripped(self) -> None:
        mgr = BinanceWsManager(
            streams=[StreamConfig("x", _noop_handler)],
            base_ws_url="wss://example.com/stream/",
        )
        assert mgr.build_url() == "wss://example.com/stream?streams=x"


class TestHealthSnapshot:
    def test_initial_state(self) -> None:
        snap = BinanceWsManager(
            streams=[StreamConfig("a@b", _noop_handler)]
        ).health_snapshot()
        assert snap["connected"] is False
        assert snap["reconnect_count"] == 0
        assert snap["total_messages"] == 0
        assert snap["total_routing_errors"] == 0
        assert snap["stream_count"] == 1
        assert snap["streams"] == ["a@b"]


# ---------------------------------------------------------------------------
# Message routing (unit tests via handle_text + MessageStats)
# ---------------------------------------------------------------------------


class TestRouting:
    @pytest.mark.asyncio
    async def test_routes_to_correct_handler(self) -> None:
        received: list[dict[str, object]] = []

        async def handler(data: Mapping[str, object]) -> None:
            received.append(dict(data))

        handlers = {"eth@trade": handler}
        stats = MessageStats()
        await handle_text(
            json.dumps({"stream": "eth@trade", "data": {"p": "100"}}),
            handlers,
            stats,
        )
        assert received == [{"p": "100"}]
        assert stats.total_messages == 1

    @pytest.mark.asyncio
    async def test_unknown_stream_ignored(self) -> None:
        handlers = {"eth@trade": _noop_handler}
        stats = MessageStats()
        await handle_text(
            json.dumps({"stream": "btc@trade", "data": {}}), handlers, stats
        )
        assert stats.total_messages == 0

    @pytest.mark.asyncio
    async def test_missing_stream_field_ignored(self) -> None:
        stats = MessageStats()
        await handle_text(json.dumps({"data": {"x": 1}}), {"a@b": _noop_handler}, stats)
        assert stats.total_messages == 0

    @pytest.mark.asyncio
    async def test_missing_data_field_ignored(self) -> None:
        stats = MessageStats()
        await handle_text(json.dumps({"stream": "a@b"}), {"a@b": _noop_handler}, stats)
        assert stats.total_messages == 0

    @pytest.mark.asyncio
    async def test_handler_error_tracked_not_crash(self) -> None:
        async def bad_handler(data: Mapping[str, object]) -> None:
            raise RuntimeError("handler boom")

        stats = MessageStats()
        await handle_text(
            json.dumps({"stream": "s", "data": {}}), {"s": bad_handler}, stats
        )
        assert stats.total_routing_errors == 1
        assert stats.total_messages == 0

    @pytest.mark.asyncio
    async def test_invalid_json_ignored(self) -> None:
        stats = MessageStats()
        await handle_text("not json {{{", {"a@b": _noop_handler}, stats)
        assert stats.total_messages == 0

    @pytest.mark.asyncio
    async def test_non_dict_payload_ignored(self) -> None:
        stats = MessageStats()
        await handle_text(json.dumps([1, 2, 3]), {"a@b": _noop_handler}, stats)
        assert stats.total_messages == 0


# ---------------------------------------------------------------------------
# Lifecycle tests (connect, receive, stop, reconnect)
# ---------------------------------------------------------------------------


class TestLifecycle:
    @pytest.mark.asyncio
    async def test_connect_receive_and_stop(self) -> None:
        received: list[dict[str, object]] = []

        async def handler(data: Mapping[str, object]) -> None:
            received.append(dict(data))

        ws = _FakeWs([_stream_msg("s", {"v": 1}), _stream_msg("s", {"v": 2})])
        mgr = BinanceWsManager(
            streams=[StreamConfig("s", handler)],
            reconnect_base_seconds=0.01,
        )
        with patch("aiohttp.ClientSession", return_value=_mock_session_ctx(ws)):
            task = asyncio.create_task(mgr.run())
            await asyncio.wait_for(mgr.wait_ready(), timeout=5.0)
            await asyncio.sleep(0.1)
            await mgr.stop()
            await asyncio.wait_for(task, timeout=5.0)

        assert len(received) == 2
        assert mgr.health_snapshot()["total_messages"] == 2

    @pytest.mark.asyncio
    async def test_reconnects_after_disconnect(self) -> None:
        received: list[dict[str, object]] = []

        async def handler(data: Mapping[str, object]) -> None:
            received.append(dict(data))

        # Second WS: yields one message then blocks until closed.
        stop_event = asyncio.Event()

        class _BlockingWs:
            def __init__(self) -> None:
                self.closed = False
                self._sent = False

            def __aiter__(self) -> _BlockingWs:
                return self

            async def __anext__(self) -> _Msg:
                if not self._sent:
                    self._sent = True
                    return _stream_msg("s", {"v": 2})
                await stop_event.wait()
                raise StopAsyncIteration

            async def close(self) -> None:
                self.closed = True
                stop_event.set()

        call_count = 0

        async def mock_connect(url: str, **kwargs: Any) -> object:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return _FakeWs([_stream_msg("s", {"v": 1})])
            return _BlockingWs()

        mgr = BinanceWsManager(
            streams=[StreamConfig("s", handler)],
            reconnect_base_seconds=0.01,
            reconnect_jitter_ratio=0.0,
        )
        with patch(
            "aiohttp.ClientSession",
            return_value=_mock_session_ctx(mock_connect),
        ):
            task = asyncio.create_task(mgr.run())
            await asyncio.wait_for(mgr.wait_ready(), timeout=5.0)
            for _ in range(200):
                if len(received) >= 2:
                    break
                await asyncio.sleep(0.01)
            await mgr.stop()
            await asyncio.wait_for(task, timeout=5.0)

        assert len(received) == 2
        assert mgr._reconnect_count >= 1

    @pytest.mark.asyncio
    async def test_startup_failure_after_max_attempts(self) -> None:
        session_mock = AsyncMock()
        session_mock.ws_connect = AsyncMock(side_effect=ConnectionError("no server"))
        mgr = BinanceWsManager(
            streams=[StreamConfig("a@b", _noop_handler)],
            max_startup_failures=3,
            reconnect_base_seconds=0.01,
            reconnect_jitter_ratio=0.0,
        )
        with patch(
            "aiohttp.ClientSession",
            return_value=_mock_session_ctx(session_mock.ws_connect),
        ):
            with pytest.raises(RuntimeError, match="3 consecutive"):
                await mgr.run()

    @pytest.mark.asyncio
    async def test_wait_ready_raises_on_startup_failure(self) -> None:
        session_mock = AsyncMock()
        session_mock.ws_connect = AsyncMock(side_effect=ConnectionError("fail"))
        mgr = BinanceWsManager(
            streams=[StreamConfig("a@b", _noop_handler)],
            max_startup_failures=2,
            reconnect_base_seconds=0.01,
            reconnect_jitter_ratio=0.0,
        )
        with patch(
            "aiohttp.ClientSession",
            return_value=_mock_session_ctx(session_mock.ws_connect),
        ):
            task = asyncio.create_task(mgr.run())
            with pytest.raises(RuntimeError, match="consecutive"):
                await asyncio.wait_for(mgr.wait_ready(), timeout=5.0)
            with pytest.raises(RuntimeError):
                await task

    @pytest.mark.asyncio
    async def test_close_message_ends_receive_loop(self) -> None:
        received: list[dict[str, object]] = []

        async def handler(data: Mapping[str, object]) -> None:
            received.append(dict(data))

        ws = _FakeWs(
            [
                _stream_msg("s", {"v": 1}),
                _Msg(aiohttp.WSMsgType.CLOSE, 1000),
                _stream_msg("s", {"v": 2}),  # should not be reached
            ]
        )
        handlers = {"s": handler}
        stats = MessageStats()
        await receive_loop(ws, handlers, stats, asyncio.Event())
        assert len(received) == 1
