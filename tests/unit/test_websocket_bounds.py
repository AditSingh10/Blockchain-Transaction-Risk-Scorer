from __future__ import annotations

import pytest

from services.api_gateway.broadcaster import RedisStreamBroadcaster
from services.api_gateway.main import (
    annotate_delivery,
    health_live,
    personalize,
    validate_tx_id,
)
from shared.config.settings import Settings


class UnusedRedis:
    pass


def test_threshold_is_presentation_only() -> None:
    message = {
        "type": "transaction",
        "data": {"tx_id": "1001", "illicit_probability": 0.82},
    }
    assert personalize(message, 0.8)["data"]["flagged"] is True
    assert personalize(message, 0.9)["data"]["flagged"] is False
    assert message["data"].get("flagged") is None


def test_slow_client_is_disconnected_when_bounded_queue_fills() -> None:
    settings = Settings(websocket_client_queue_size=8)
    broadcaster = RedisStreamBroadcaster(UnusedRedis(), settings)
    client = broadcaster.register()
    for sequence in range(9):
        broadcaster._broadcast(
            f"1-{sequence}",
            {"type": "transaction", "data": {"tx_id": str(sequence)}},
        )
    assert client.closed.is_set()
    assert client not in broadcaster.clients


def test_transaction_id_validation() -> None:
    assert validate_tx_id("abc_123-def") == "abc_123-def"


def test_delivery_annotations_distinguish_cursor_replay() -> None:
    message = {
        "type": "transaction",
        "stream_id": "1-0",
        "data": {
            "source_ingested_at": "2026-01-01T00:00:00+00:00",
            "illicit_probability": 0.5,
        },
    }
    annotated = annotate_delivery(message, replayed=True)
    assert annotated["data"]["delivery_mode"] == "replay"
    assert annotated["data"]["redis_to_websocket_latency_ms"] >= 0
    assert annotated["data"]["end_to_end_latency_ms"] >= 0


@pytest.mark.asyncio
async def test_liveness_is_dependency_independent() -> None:
    assert await health_live() == {"status": "alive"}
