from __future__ import annotations

import asyncio
import json
import os

import httpx
import pytest
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
    async with httpx.AsyncClient(base_url=api_url, timeout=5) as client:
        ready = await client.get("/health/ready")
        assert ready.status_code == 200
        async with websockets.connect(f"{websocket_url}?last_event_id=0-0") as first:
            async with websockets.connect(f"{websocket_url}?last_event_id=0-0") as second:
                first_message, second_message = await asyncio.gather(
                    connected_and_transaction(first),
                    connected_and_transaction(second),
                )
        first_gateway, first_event = first_message
        second_gateway, second_event = second_message
        assert first_gateway
        assert second_gateway
        assert first_gateway != second_gateway
        assert first_event["event_id"] == second_event["event_id"]
        assert first_event["stream_id"] == second_event["stream_id"]
        tx_id = first_event["data"]["tx_id"]
        entity = await client.get(f"/api/v1/entity/{tx_id}")
        subgraph = await client.get(f"/api/v1/subgraph/{tx_id}")
        assert entity.status_code == 200
        assert subgraph.status_code == 200
        assert subgraph.json()["center"] == tx_id
        assert "truncated" in subgraph.json()
