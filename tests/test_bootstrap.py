from __future__ import annotations

from src.config.bootstrap import load_bootstrap_config


def test_bootstrap_requires_opt_in_for_non_transactional_migrations(
    monkeypatch,
) -> None:
    monkeypatch.delenv("COLLECTOR_ALLOW_NON_TRANSACTIONAL_MIGRATIONS", raising=False)

    config = load_bootstrap_config()

    assert config.allow_non_transactional_migrations is False


def test_bootstrap_allows_non_transactional_migrations_with_explicit_opt_in(
    monkeypatch,
) -> None:
    monkeypatch.setenv("COLLECTOR_ALLOW_NON_TRANSACTIONAL_MIGRATIONS", "true")

    config = load_bootstrap_config()

    assert config.allow_non_transactional_migrations is True
