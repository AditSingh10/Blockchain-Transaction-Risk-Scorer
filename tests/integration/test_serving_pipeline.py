from __future__ import annotations

import asyncio
import json
import os
from contextlib import AsyncExitStack

import httpx
import pytest
import redis.asyncio as redis
import websockets

pytestmark = pytest.mark.integration


async def connected_and_transaction(websocket) -> tuple[str, dict]:
    gateway_instance = ""
    while True:
        message = json.loads(await websocket.recv())
        if message.get("type") == "connected":
            gateway_instance = message["gateway_instance"]
        elif message.get("type") == "transaction":
            return gateway_instance, message


@pytest.mark.asyncio
async def test_rest_and_two_simultaneous_websocket_clients() -> None:
    api_url = os.getenv("RISK_INTEGRATION_API_URL", "http://localhost:8000")
    websocket_url = os.getenv(
        "RISK_INTEGRATION_WS_URL",
        "ws://localhost:8000/api/v1/ws",
    )
    redis_url = os.getenv("RISK_REDIS_URL", "redis://localhost:6379/0")
    redis_stream_key = os.getenv(
        "RISK_REDIS_STREAM_KEY",
        "risk:scoring-results:v1",
    )
    redis_client = redis.from_url(redis_url, decode_responses=True)
    recent = await redis_client.xrevrange(redis_stream_key, count=2)
    await redis_client.aclose()
    assert len(recent) == 2
    replay_cursor = recent[-1][0]
    async with httpx.AsyncClient(base_url=api_url, timeout=5) as client:
        ready = await client.get("/health/ready")
        assert ready.status_code == 200
        async with AsyncExitStack() as stack:
            readers = []
            for _ in range(6):
                socket = await stack.enter_async_context(
                    websockets.connect(f"{websocket_url}?last_event_id={replay_cursor}")
                )
                readers.append(asyncio.create_task(connected_and_transaction(socket)))
            messages = await asyncio.gather(*readers)
        gateway_events = {gateway: event for gateway, event in messages if gateway}
        assert len(gateway_events) >= 2
        first_event, second_event = list(gateway_events.values())[:2]
        assert first_event["event_id"] == second_event["event_id"]
        assert first_event["stream_id"] == second_event["stream_id"]
        tx_id = first_event["data"]["tx_id"]
        entity = await client.get(f"/api/v1/entity/{tx_id}")
        subgraph = await client.get(f"/api/v1/subgraph/{tx_id}")
        assert entity.status_code == 200
        assert subgraph.status_code == 200
        assert subgraph.json()["center"] == tx_id
        assert "truncated" in subgraph.json()
