from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .ws_bridge import HyperliquidWsBridge


def build_subscription_health(
    bridge: HyperliquidWsBridge,
) -> dict[str, dict[str, object]]:
    current_gap_started_at = (
        bridge._current_gap_started_at.isoformat()
        if bridge._current_gap_started_at is not None
        else None
    )
    return {
        key: {
            "source_id": bridge.subscriptions[key].source_id or key,
            "last_message_at": bridge.last_message_at_by_key[key],
            "current_gap_started_at": current_gap_started_at,
            "last_gap_started_at": bridge.last_gap_started_at,
            "last_gap_ended_at": bridge.last_gap_ended_at,
            "last_gap_duration_seconds": bridge.last_gap_duration_seconds,
            "queue_size": bridge.queues[key].qsize(),
        }
        for key in bridge.subscriptions
    }


def build_source_health(
    bridge: HyperliquidWsBridge,
) -> dict[str, dict[str, object]]:
    aggregated: dict[str, dict[str, object]] = {}
    for key, snapshot in build_subscription_health(bridge).items():
        source_id = snapshot["source_id"]
        existing = aggregated.setdefault(
            source_id,
            {
                "last_message_at": None,
                "current_gap_started_at": None,
                "last_gap_started_at": None,
                "last_gap_ended_at": None,
                "last_gap_duration_seconds": None,
                "queue_size": 0,
                "subscriptions": [],
            },
        )
        existing["queue_size"] += snapshot["queue_size"]
        existing["subscriptions"].append(key)
        if snapshot["last_message_at"] is not None and (
            existing["last_message_at"] is None
            or snapshot["last_message_at"] > existing["last_message_at"]
        ):
            existing["last_message_at"] = snapshot["last_message_at"]
    return aggregated
