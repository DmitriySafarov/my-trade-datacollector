from __future__ import annotations

import importlib

import pytest

import tests._db_test_support as db_test_support

TEST_DB_ENV_VARS = (
    "TEST_DB_HOST",
    "TEST_DB_PORT",
    "TEST_DB_BOOTSTRAP",
    "TEST_DB_SOCKET_DIR",
)


@pytest.fixture
def reloaded_db_test_support(monkeypatch: pytest.MonkeyPatch):
    def _reload() -> object:
        return importlib.reload(db_test_support)

    yield _reload
    for name in TEST_DB_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    importlib.reload(db_test_support)


def test_compose_defaults_try_socket_then_tcp(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    reloaded_db_test_support,
) -> None:
    monkeypatch.delenv("TEST_DB_HOST", raising=False)
    monkeypatch.delenv("TEST_DB_PORT", raising=False)
    monkeypatch.delenv("TEST_DB_BOOTSTRAP", raising=False)
    monkeypatch.setenv("TEST_DB_SOCKET_DIR", str(tmp_path / "postgres-socket"))

    support = reloaded_db_test_support()

    assert (
        support._test_config("collector").db_host,
        support._test_config("collector").db_port,
    ) == (
        "127.0.0.1",
        55432,
    )
    assert [
        (candidate.db_host, candidate.db_port)
        for candidate in support._test_configs("collector")
    ] == [(str(tmp_path / "postgres-socket"), 5432), ("127.0.0.1", 55432)]
    assert support.DOCKER_ENV["DB_PORT"] == "55432"


def test_explicit_external_override_uses_only_requested_transport(
    monkeypatch: pytest.MonkeyPatch,
    reloaded_db_test_support,
) -> None:
    monkeypatch.setenv("TEST_DB_HOST", "postgres")
    monkeypatch.setenv("TEST_DB_PORT", "5432")
    monkeypatch.setenv("TEST_DB_BOOTSTRAP", "external")

    support = reloaded_db_test_support()

    assert (
        support._test_config("collector").db_host,
        support._test_config("collector").db_port,
    ) == (
        "postgres",
        5432,
    )
    assert [
        (candidate.db_host, candidate.db_port)
        for candidate in support._test_configs("collector")
    ] == [("postgres", 5432)]


def test_compose_port_override_does_not_change_socket_candidate(
    monkeypatch: pytest.MonkeyPatch,
    reloaded_db_test_support,
) -> None:
    monkeypatch.delenv("TEST_DB_HOST", raising=False)
    monkeypatch.setenv("TEST_DB_PORT", "6543")
    monkeypatch.setenv("TEST_DB_BOOTSTRAP", "compose")

    support = reloaded_db_test_support()

    assert [
        (candidate.db_host, candidate.db_port)
        for candidate in support._test_configs("collector")
    ] == [(str(support.TEST_DB_SOCKET_DIR), 5432), ("127.0.0.1", 6543)]
    assert support.DOCKER_ENV["DB_PORT"] == "6543"


def test_explicit_socket_host_keeps_compose_published_port_stable(
    monkeypatch: pytest.MonkeyPatch,
    reloaded_db_test_support,
) -> None:
    monkeypatch.setenv("TEST_DB_HOST", "/tmp/postgres-socket")
    monkeypatch.delenv("TEST_DB_PORT", raising=False)
    monkeypatch.setenv("TEST_DB_BOOTSTRAP", "compose")

    support = reloaded_db_test_support()

    assert [
        (candidate.db_host, candidate.db_port)
        for candidate in support._test_configs("collector")
    ] == [("/tmp/postgres-socket", 5432)]
    assert support.DOCKER_ENV["DB_PORT"] == "55432"
