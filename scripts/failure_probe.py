#!/usr/bin/env python3
"""Issue and verify a durable scoring request around an external worker crash."""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from datetime import UTC, datetime
from uuid import uuid4

import asyncpg

from shared.config.settings import Settings
from shared.contracts.events import ScoringRequestedPayloadV1, ScoringRequestedV1
from shared.contracts.ids import deterministic_event_id
from shared.kafka.client import build_producer


async def issue(settings: Settings) -> None:
    connection = await asyncpg.connect(
        settings.postgres_dsn.replace("postgresql+asyncpg://", "postgresql://", 1)
    )
    try:
        row = await connection.fetchrow(
            """
            SELECT tx_id, graph_watermark, time_step, ingested_at
            FROM transactions
            ORDER BY created_at
            LIMIT 1
            """
        )
    finally:
        await connection.close()
    if row is None:
        raise RuntimeError("failure probe requires a materialized transaction")
    event_id = str(uuid4())
    result_id = deterministic_event_id(
        "risk.scoring.completed.v1",
        event_id,
        settings.model_version,
    )
    event = ScoringRequestedV1(
        event_id=event_id,
        occurred_at=datetime.now(UTC),
        producer="failure-probe",
        trace_id=uuid4().hex,
        correlation_id=f"failure-probe:{event_id}",
        payload=ScoringRequestedPayloadV1(
            tx_id=row["tx_id"],
            graph_watermark=row["graph_watermark"],
            source_time_step=row["time_step"],
            event_sequence=999_999,
            feature_schema_version=settings.feature_schema_version,
            source_ingested_at=datetime.now(UTC),
            requested_at=datetime.now(UTC),
        ),
    )
    producer = build_producer(settings, client_id="failure-probe")
    await producer.start()
    try:
        await producer.send_and_wait(
            settings.scoring_request_topic,
            key=str(row["tx_id"]).encode(),
            value=event.model_dump_json().encode(),
            headers=event.kafka_headers(),
        )
    finally:
        await producer.stop()
    print(json.dumps({"request_event_id": event_id, "result_event_id": result_id}))


async def verify(
    settings: Settings,
    result_event_id: str,
    timeout_seconds: float,
) -> None:
    connection = await asyncpg.connect(
        settings.postgres_dsn.replace("postgresql+asyncpg://", "postgresql://", 1)
    )
    deadline = time.monotonic() + timeout_seconds
    try:
        while time.monotonic() < deadline:
            found = await connection.fetchval(
                "SELECT EXISTS (SELECT 1 FROM risk_scores WHERE source_event_id = $1)",
                result_event_id,
            )
            if found:
                print(json.dumps({"recovered": True, "result_event_id": result_event_id}))
                return
            await asyncio.sleep(1)
    finally:
        await connection.close()
    raise RuntimeError("worker restart did not produce the accepted scoring request")


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("issue")
    verify_parser = subparsers.add_parser("verify")
    verify_parser.add_argument("--result-event-id", required=True)
    verify_parser.add_argument("--timeout", type=float, default=120)
    args = parser.parse_args()
    settings = Settings(service_name="failure-probe")
    if args.command == "issue":
        asyncio.run(issue(settings))
    else:
        asyncio.run(verify(settings, args.result_event_id, args.timeout))


if __name__ == "__main__":
    main()
