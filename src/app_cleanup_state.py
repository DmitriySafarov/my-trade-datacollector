from __future__ import annotations

from src.collectors.base import BaseCollector


class CollectorStartupInterrupted(RuntimeError):
    pass


def attach_cleanup_state(
    error: BaseException,
    collectors: list[BaseCollector],
) -> None:
    if collectors:
        setattr(error, "_collector_cleanup_required", list(collectors))


def cleanup_required_collectors(error: BaseException) -> list[BaseCollector]:
    return list(getattr(error, "_collector_cleanup_required", []))
