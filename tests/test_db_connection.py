from __future__ import annotations

from dataclasses import replace

import asyncpg
import pytest

import tests._db_connection as db_connection
from tests._db_test_support import _test_config


@pytest.mark.asyncio
async def test_wait_for_database_retries_viable_tcp_after_socket_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    socket_candidate = replace(_test_config("collector"), db_host="/socket")
    tcp_candidate = replace(_test_config("collector"), db_host="tcp-host")
    attempts: list[str] = []
    sleep_calls: list[float] = []
    tcp_attempts = 0

    async def fake_connect_once(config) -> None:
        nonlocal tcp_attempts
        attempts.append(config.db_host)
        if config.db_host == "/socket":
            raise PermissionError(db_connection.EPERM, "socket access blocked")
        tcp_attempts += 1
        if tcp_attempts == 1:
            raise OSError("connection refused")

    async def fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    monkeypatch.setattr(db_connection, "_connect_once", fake_connect_once)
    monkeypatch.setattr(db_connection.asyncio, "sleep", fake_sleep)

    selected = await db_connection.wait_for_database(
        [socket_candidate, tcp_candidate],
        timeout=60.0,
    )

    assert selected == tcp_candidate
    assert attempts == ["/socket", "tcp-host", "/socket", "tcp-host"]
    assert sleep_calls == [1]


@pytest.mark.asyncio
async def test_wait_for_database_returns_tcp_when_it_succeeds_in_same_pass(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    socket_candidate = replace(_test_config("collector"), db_host="/socket")
    tcp_candidate = replace(_test_config("collector"), db_host="tcp-host")
    attempts: list[str] = []

    async def fake_connect_once(config) -> None:
        attempts.append(config.db_host)
        if config.db_host == "/socket":
            raise PermissionError(db_connection.EPERM, "socket access blocked")

    async def fail_sleep(delay: float) -> None:
        del delay
        raise AssertionError("wait_for_database should not sleep after success")

    monkeypatch.setattr(db_connection, "_connect_once", fake_connect_once)
    monkeypatch.setattr(db_connection.asyncio, "sleep", fail_sleep)

    assert (
        await db_connection.wait_for_database([socket_candidate, tcp_candidate])
        == tcp_candidate
    )
    assert attempts == ["/socket", "tcp-host"]


@pytest.mark.asyncio
async def test_wait_for_database_stops_when_every_candidate_is_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    socket_candidate = replace(_test_config("collector"), db_host="/socket")
    auth_candidate = replace(_test_config("collector"), db_host="tcp-host")
    attempts: list[str] = []

    async def fake_connect_once(config) -> None:
        attempts.append(config.db_host)
        if config.db_host == "/socket":
            raise PermissionError(db_connection.EPERM, "socket access blocked")
        raise asyncpg.InvalidPasswordError("bad password")

    async def fail_sleep(delay: float) -> None:
        del delay
        raise AssertionError("wait_for_database should not retry fatal candidates")

    monkeypatch.setattr(db_connection, "_connect_once", fake_connect_once)
    monkeypatch.setattr(db_connection.asyncio, "sleep", fail_sleep)

    with pytest.raises(RuntimeError, match="Pinned Docker Postgres credentials"):
        await db_connection.wait_for_database(
            [socket_candidate, auth_candidate],
            timeout=60.0,
        )

    assert attempts == ["/socket", "tcp-host"]
