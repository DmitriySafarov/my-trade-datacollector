from __future__ import annotations

import asyncio
import threading

import pytest

from src.collectors.hyperliquid import HyperliquidMarketCollector
import src.collectors.hyperliquid.lifecycle_support as lifecycle_support

from ._ws_test_support import FakeHyperliquidSession, FakeHyperliquidSessionFactory
from .test_market_l2book import _load_fixture_messages
from .test_market_l2book import _start_l2book_collector


class _DelayedCloseSession(FakeHyperliquidSession):
    def __init__(
        self,
        on_open,
        on_close,
        on_error,
        *,
        close_started: threading.Event,
        release_close: threading.Event,
    ) -> None:
        super().__init__(on_open, on_close, on_error)
        self._close_started = close_started
        self._release_close = release_close

    def close(self) -> None:
        self._close_started.set()
        self._release_close.wait(timeout=1.0)
        self.closed = True


@pytest.mark.asyncio
async def test_market_collector_cancellation_preserves_primary_error_when_close_fails(
    migrated_db: dict[str, object],
) -> None:
    pool = migrated_db["pool"]
    collector, task, session, _factory = await _start_l2book_collector(pool)

    async def failing_close() -> None:
        raise RuntimeError("close boom")

    assert collector._l2book_writer is not None
    collector._l2book_writer.close = failing_close  # type: ignore[method-assign]
    task.cancel()

    with pytest.raises(asyncio.CancelledError) as raised:
        await asyncio.wait_for(task, timeout=1.0)

    assert session.closed is True
    assert any("close boom" in note for note in raised.value.__notes__ or [])


@pytest.mark.asyncio
async def test_market_collector_drops_l2book_message_emitted_after_stop_starts(
    migrated_db: dict[str, object],
) -> None:
    pool = migrated_db["pool"]
    close_started = threading.Event()
    release_close = threading.Event()
    factory = FakeHyperliquidSessionFactory(
        session_builder=lambda on_open, on_close, on_error: _DelayedCloseSession(
            on_open,
            on_close,
            on_error,
            close_started=close_started,
            release_close=release_close,
        )
    )
    collector = HyperliquidMarketCollector(
        base_url="https://api.hyperliquid.xyz",
        pool=pool,
        count_limit=50,
        time_limit_seconds=60.0,
        enable_trades=False,
        enable_l2book=True,
        session_factory=factory,
    )
    task = asyncio.create_task(collector.start())
    session = await factory.next_session()

    session.open()
    await collector.wait_ready()

    stop_task = asyncio.create_task(collector.stop())
    assert await asyncio.wait_for(asyncio.to_thread(close_started.wait, 1.0), 1.0)

    session.emit("l2Book:eth", _load_fixture_messages()[0])
    release_close.set()

    await asyncio.wait_for(stop_task, timeout=1.0)
    await asyncio.wait_for(task, timeout=1.0)

    async with pool.acquire() as connection:
        row_count = await connection.fetchval(
            """
            SELECT count(*)
            FROM hl_l2book
            WHERE source = 'hl_ws_l2book'
              AND coin = 'ETH'
            """,
        )

    assert row_count == 0


@pytest.mark.asyncio
async def test_market_collector_stop_aborts_hung_writer_close(
    migrated_db: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = migrated_db["pool"]
    collector, task, session, _factory = await _start_l2book_collector(pool)
    cancelled = asyncio.Event()

    async def hanging_flush(payload: list[tuple[object, ...]]) -> None:
        del payload
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    assert collector._l2book_writer is not None
    session.emit("l2Book:eth", _load_fixture_messages()[0])
    while collector._l2book_writer.snapshot()["buffer_size"] != 1:
        await asyncio.sleep(0.01)
    collector._l2book_writer.flush_callback = hanging_flush  # type: ignore[assignment]
    monkeypatch.setattr(lifecycle_support, "WRITER_CLOSE_TIMEOUT_SECONDS", 0.01)

    await asyncio.wait_for(collector.stop(), timeout=1.0)
    with pytest.raises(
        RuntimeError,
        match="BatchWriter hyperliquid_l2book_writer close timed out after 0.01s",
    ):
        await asyncio.wait_for(task, timeout=1.0)
    await asyncio.wait_for(cancelled.wait(), timeout=1.0)
    assert session.closed is True
