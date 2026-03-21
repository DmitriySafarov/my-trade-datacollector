from __future__ import annotations

from abc import ABC, abstractmethod


class BaseCollector(ABC):
    name: str
    source_ids: tuple[str, ...]

    @abstractmethod
    async def start(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def stop(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def health_snapshot(self) -> dict[str, object]:
        raise NotImplementedError
