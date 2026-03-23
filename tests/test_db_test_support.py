from __future__ import annotations

import importlib

import pytest

import tests._db_test_support as db_test_support
from src.db.pool import close_pool, create_pool

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


@pytest.mark.asyncio
async def test_ensure_postgres_service_starts_compose_before_waiting(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    reloaded_db_test_support,
) -> None:
    support = reloaded_db_test_support()
    monkeypatch.setattr(support, "TEST_DB_SOCKET_DIR", tmp_path / "postgres-socket")
    events: list[tuple[str, object]] = []

    async def fake_wait(
        database: str,
        *,
        timeout: float = 60.0,
        allow_missing_database: bool = False,
    ) -> None:
        del allow_missing_database
        events.append(("wait", (database, timeout)))

    class _Process:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"postgres ready", b""

        def kill(self) -> None:
            return None

        async def wait(self) -> int:
            return 0

    async def fake_exec(*args, **kwargs):
        del kwargs
        events.append(("compose", args))
        return _Process()

    monkeypatch.setattr(support, "_wait_for_database", fake_wait)
    monkeypatch.setattr(support.asyncio, "create_subprocess_exec", fake_exec)

    await support._ensure_postgres_service()

    assert events == [
        ("compose", ("docker", "compose", "up", "-d", "postgres")),
        ("wait", ("postgres", 60.0)),
    ]


@pytest.mark.asyncio
async def test_ensure_postgres_service_propagates_post_bootstrap_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    reloaded_db_test_support,
) -> None:
    support = reloaded_db_test_support()
    monkeypatch.setattr(support, "TEST_DB_SOCKET_DIR", tmp_path / "postgres-socket")
    compose_calls: list[tuple[str, ...]] = []

    async def fake_wait(
        database: str,
        *,
        timeout: float = 60.0,
        allow_missing_database: bool = False,
    ) -> None:
        del database, timeout, allow_missing_database
        raise RuntimeError(
            "Sandbox blocked local PostgreSQL socket access during DB tests."
        )

    async def fake_exec(*args, **kwargs):
        del kwargs
        compose_calls.append(args)

        class _Process:
            returncode = 0

            async def communicate(self) -> tuple[bytes, bytes]:
                return b"postgres ready", b""

        return _Process()

    monkeypatch.setattr(support, "_wait_for_database", fake_wait)
    monkeypatch.setattr(support.asyncio, "create_subprocess_exec", fake_exec)

    with pytest.raises(RuntimeError, match="Sandbox blocked local PostgreSQL"):
        await support._ensure_postgres_service()
    assert compose_calls == [("docker", "compose", "up", "-d", "postgres")]


@pytest.mark.asyncio
async def test_ensure_postgres_service_skips_compose_for_explicit_host_override(
    monkeypatch: pytest.MonkeyPatch,
    reloaded_db_test_support,
) -> None:
    monkeypatch.setenv("TEST_DB_HOST", "postgres")
    support = reloaded_db_test_support()
    events: list[tuple[str, object]] = []

    async def fake_wait(
        database: str,
        *,
        timeout: float = 60.0,
        allow_missing_database: bool = False,
    ) -> None:
        del allow_missing_database
        events.append(("wait", (database, timeout)))

    async def fake_exec(*args, **kwargs):
        del args, kwargs
        raise AssertionError("docker compose should not run with explicit TEST_DB_HOST")

    monkeypatch.setattr(support, "_wait_for_database", fake_wait)
    monkeypatch.setattr(support.asyncio, "create_subprocess_exec", fake_exec)

    await support._ensure_postgres_service()

    assert events == [("wait", ("postgres", 60.0))]


@pytest.mark.asyncio
async def test_ensure_postgres_service_surfaces_external_fallback_on_docker_permission_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    reloaded_db_test_support,
) -> None:
    support = reloaded_db_test_support()
    monkeypatch.setattr(support, "TEST_DB_SOCKET_DIR", tmp_path / "postgres-socket")

    async def fake_exec(*args, **kwargs):
        del args, kwargs

        class _Process:
            returncode = 1

            async def communicate(self) -> tuple[bytes, bytes]:
                return (
                    b"",
                    b"permission denied while trying to connect to the Docker daemon socket at unix:///Users/test/.docker/run/docker.sock: connect: operation not permitted",
                )

        return _Process()

    monkeypatch.setattr(support.asyncio, "create_subprocess_exec", fake_exec)

    with pytest.raises(RuntimeError, match="TEST_DB_BOOTSTRAP=external"):
        await support._ensure_postgres_service()


@pytest.mark.asyncio
async def test_migrated_db_returns_reusable_selected_transport(
    migrated_db: dict[str, object],
) -> None:
    pool = await create_pool(migrated_db["config"])
    try:
        async with pool.acquire() as connection:
            assert await connection.fetchval("SELECT 1") == 1
    finally:
        await close_pool(pool)
