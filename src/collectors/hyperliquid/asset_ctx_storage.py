from __future__ import annotations

from collections.abc import Sequence

import asyncpg


class HyperliquidAssetCtxStore:
    _COLUMNS = (
        "time",
        "source",
        "coin",
        "funding",
        "open_interest",
        "mark_price",
        "oracle_price",
        "mid_price",
        "premium",
        "buy_impact_price",
        "sell_impact_price",
        "day_notional_volume",
        "prev_day_price",
        "event_hash",
        "payload",
    )

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def write_many(self, records: Sequence[tuple[object, ...]]) -> None:
        if not records:
            return
        async with self._pool.acquire() as connection:
            await connection.copy_records_to_table(
                "hl_asset_ctx",
                records=records,
                columns=self._COLUMNS,
            )
