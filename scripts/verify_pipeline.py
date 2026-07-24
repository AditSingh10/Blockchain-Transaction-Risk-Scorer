#!/usr/bin/env python3
"""Verify durable state, Redis publication, REST lookup, and WebSocket replay."""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from urllib.request import urlopen

import asyncpg
import redis.asyncio as redis
import websockets


def postgres_url(dsn: str) -> str:
    return dsn.replace("postgresql+asyncpg://", "postgresql://", 1)


def read_json(url: str) -> dict:
    with urlopen(url, timeout=3) as response:
        return json.load(response)


async def verify(args: argparse.Namespace) -> None:
    deadline = time.monotonic() + args.timeout
    pool = await asyncpg.create_pool(postgres_url(args.postgres_dsn), min_size=1, max_size=2)
    redis_client = redis.from_url(args.redis_url, decode_responses=True)
    row = None
    try:
        while time.monotonic() < deadline:
            async with pool.acquire() as connection:
                row = await connection.fetchrow(
                    """
                    SELECT rs.tx_id, rs.model_version, rs.illicit_probability
                    FROM risk_scores rs
                    ORDER BY rs.scored_at DESC
                    LIMIT 1
                    """
                )
            if row and await redis_client.xlen(args.stream_key) > 0:
                break
            await asyncio.sleep(1)
        if row is None:
            raise RuntimeError("no canonical risk score reached PostgreSQL")
        if await redis_client.xlen(args.stream_key) == 0:
            raise RuntimeError("no result reached the Redis Stream")

        health = await asyncio.to_thread(read_json, f"{args.api_url}/health/ready")
        if health.get("status") != "ready":
            raise RuntimeError(f"gateway not ready: {health}")
        entity = await asyncio.to_thread(
            read_json,
            f"{args.api_url}/api/v1/entity/{row['tx_id']}",
        )
        if entity["model_version"] != row["model_version"]:
            raise RuntimeError("REST entity response does not match canonical score")

        websocket_url = args.websocket_url + "?last_event_id=0-0"
        async with websockets.connect(websocket_url, open_timeout=5) as websocket:
            while True:
                message = json.loads(await asyncio.wait_for(websocket.recv(), timeout=5))
                if message.get("type") == "transaction":
                    if not message.get("stream_id"):
                        raise RuntimeError("WebSocket event has no Redis Stream cursor")
                    break
        print(
            json.dumps(
                {
                    "status": "verified",
                    "tx_id": row["tx_id"],
                    "model_version": row["model_version"],
                    "redis_stream_length": await redis_client.xlen(args.stream_key),
                    "websocket_stream_id": message["stream_id"],
                },
                indent=2,
            )
        )
    finally:
        await redis_client.close()
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--postgres-dsn",
        default="postgresql+asyncpg://risk:risk@localhost:5432/risk",
    )
    parser.add_argument("--redis-url", default="redis://localhost:6379/0")
    parser.add_argument("--stream-key", default="risk:scoring-results:v1")
    parser.add_argument("--api-url", default="http://localhost:8000")
    parser.add_argument("--websocket-url", default="ws://localhost:8000/api/v1/ws")
    parser.add_argument("--timeout", type=float, default=180)
    asyncio.run(verify(parser.parse_args()))


if __name__ == "__main__":
    main()
