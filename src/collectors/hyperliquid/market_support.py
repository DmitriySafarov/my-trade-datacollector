from __future__ import annotations

from collections.abc import Sequence
from functools import partial

from src.db.batch_writer import BatchWriter

from .candle_parsing import VALID_INTERVALS
from .market_handlers import (
    ASSET_CTX_SOURCE_ID,
    CANDLES_SOURCE_ID,
    L2BOOK_SOURCE_ID,
    TRADES_SOURCE_ID,
    handle_asset_ctx_message,
    handle_candle_message,
    handle_l2book_message,
    handle_trades_message,
)
from .ws_subscription import HyperliquidWsSubscription


def build_market_subscriptions(
    *,
    coins: Sequence[str],
    trade_writer: BatchWriter[tuple[object, ...]] | None,
    l2book_writer: BatchWriter[tuple[object, ...]] | None,
    asset_ctx_writer: BatchWriter[tuple[object, ...]] | None,
    candle_writer: BatchWriter[tuple[object, ...]] | None = None,
    candle_intervals: Sequence[str] | None = None,
) -> list[HyperliquidWsSubscription]:
    subscriptions: list[HyperliquidWsSubscription] = []
    intervals = tuple(candle_intervals) if candle_intervals else VALID_INTERVALS
    if candle_writer is not None:
        invalid = set(intervals) - set(VALID_INTERVALS)
        if invalid:
            raise ValueError(f"Unsupported candle intervals: {sorted(invalid)}")
    for coin in coins:
        if trade_writer is not None:
            subscriptions.append(
                HyperliquidWsSubscription(
                    name=f"trades:{coin.lower()}",
                    subscription={"type": "trades", "coin": coin},
                    handler=partial(
                        handle_trades_message,
                        writer=trade_writer,
                        allowed_coins=(coin,),
                    ),
                    source_id=TRADES_SOURCE_ID,
                )
            )
        if l2book_writer is not None:
            subscriptions.append(
                HyperliquidWsSubscription(
                    name=f"l2Book:{coin.lower()}",
                    subscription={"type": "l2Book", "coin": coin},
                    handler=partial(
                        handle_l2book_message,
                        writer=l2book_writer,
                        allowed_coins=(coin,),
                    ),
                    source_id=L2BOOK_SOURCE_ID,
                )
            )
        if asset_ctx_writer is not None:
            subscriptions.append(
                HyperliquidWsSubscription(
                    name=f"activeAssetCtx:{coin.lower()}",
                    subscription={"type": "activeAssetCtx", "coin": coin},
                    handler=partial(
                        handle_asset_ctx_message,
                        writer=asset_ctx_writer,
                        allowed_coins=(coin,),
                    ),
                    source_id=ASSET_CTX_SOURCE_ID,
                )
            )
        if candle_writer is not None:
            for interval in intervals:
                subscriptions.append(
                    HyperliquidWsSubscription(
                        name=f"candle:{coin.lower()},{interval}",
                        subscription={
                            "type": "candle",
                            "coin": coin,
                            "interval": interval,
                        },
                        handler=partial(
                            handle_candle_message,
                            writer=candle_writer,
                            allowed_coins=(coin,),
                        ),
                        source_id=CANDLES_SOURCE_ID,
                    )
                )
    return subscriptions
