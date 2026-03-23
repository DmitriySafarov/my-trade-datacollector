from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence

from src.db.batch_writer import BatchWriter

from .l2book_parsing import parse_hyperliquid_l2book
from .trade_parsing import parse_hyperliquid_trade


TRADES_SOURCE_ID = "hl_ws_trades"
L2BOOK_SOURCE_ID = "hl_ws_l2book"
LOGGER = logging.getLogger(__name__)


async def handle_trades_message(
    message: object,
    *,
    writer: BatchWriter[tuple[object, ...]],
    allowed_coins: Sequence[str],
) -> None:
    try:
        data = require_message_data(message, channel="trades")
    except ValueError as error:
        LOGGER.warning(
            "hyperliquid_trade_message_invalid source=%s error=%s payload=%r",
            TRADES_SOURCE_ID,
            error,
            message,
        )
        return
    if not isinstance(data, list):
        LOGGER.warning(
            "hyperliquid_trade_message_invalid source=%s error=%s payload=%r",
            TRADES_SOURCE_ID,
            "Hyperliquid trades payload must be a list",
            message,
        )
        return
    records: list[tuple[object, ...]] = []
    for index, trade in enumerate(data):
        try:
            record = parse_hyperliquid_trade(
                trade,
                source=TRADES_SOURCE_ID,
                allowed_coins=allowed_coins,
            )
        except (OSError, OverflowError, ValueError) as error:
            LOGGER.warning(
                "hyperliquid_trade_payload_invalid source=%s index=%s error=%s payload=%r",
                TRADES_SOURCE_ID,
                index,
                error,
                trade,
            )
            continue
        records.append(record.as_copy_row())
    if records:
        await writer.add_many(records)


async def handle_l2book_message(
    message: object,
    *,
    writer: BatchWriter[tuple[object, ...]],
    allowed_coins: Sequence[str],
) -> None:
    try:
        data = require_message_data(message, channel="l2Book")
        record = parse_hyperliquid_l2book(
            data,
            source=L2BOOK_SOURCE_ID,
            allowed_coins=allowed_coins,
        )
    except (OSError, OverflowError, ValueError) as error:
        LOGGER.warning(
            "hyperliquid_l2book_payload_invalid source=%s error=%s payload=%r",
            L2BOOK_SOURCE_ID,
            error,
            message,
        )
        return
    await writer.add(record.as_copy_row())


def require_message_data(message: object, *, channel: str) -> object:
    if not isinstance(message, Mapping):
        raise ValueError(f"Hyperliquid {channel} message must be a mapping")
    if message.get("channel") != channel:
        raise ValueError(f"Unexpected Hyperliquid {channel} channel")
    return message.get("data")
