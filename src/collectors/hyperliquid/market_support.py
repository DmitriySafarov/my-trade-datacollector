from __future__ import annotations

from collections.abc import Sequence
from functools import partial

from src.db.batch_writer import BatchWriter

from .market_handlers import handle_l2book_message, handle_trades_message
from .market_handlers import L2BOOK_SOURCE_ID, TRADES_SOURCE_ID
from .ws_subscription import HyperliquidWsSubscription


def build_market_subscriptions(
    *,
    coins: Sequence[str],
    trade_writer: BatchWriter[tuple[object, ...]] | None,
    l2book_writer: BatchWriter[tuple[object, ...]] | None,
) -> list[HyperliquidWsSubscription]:
    subscriptions: list[HyperliquidWsSubscription] = []
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
    return subscriptions
