#!/usr/bin/env python3
"""Inspect or explicitly replay one selected DLQ record."""

from __future__ import annotations

import argparse
import asyncio
import json
from uuid import uuid4

from aiokafka import AIOKafkaConsumer, TopicPartition

from shared.config.settings import Settings
from shared.contracts.events import DeadLetterEventV1
from shared.kafka.client import build_producer


async def run(args: argparse.Namespace) -> None:
    settings = Settings(service_name="dlq-replay")
    consumer = AIOKafkaConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        enable_auto_commit=False,
        group_id=None,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        partition = TopicPartition(settings.scoring_dlq_topic, args.partition)
        consumer.assign([partition])
        consumer.seek(partition, args.offset)
        record = await asyncio.wait_for(consumer.getone(), timeout=5)
        dead_letter = DeadLetterEventV1.model_validate_json(record.value)
        print(dead_letter.model_dump_json(indent=2))
        if not args.replay:
            return
        if not args.initiated_by:
            raise RuntimeError("--initiated-by is required with --replay")
        if not isinstance(dead_letter.payload.original_payload, dict):
            raise RuntimeError("selected DLQ payload is not a replayable event object")
        payload = dict(dead_letter.payload.original_payload)
        original_event_id = payload.get("event_id")
        payload["event_id"] = str(uuid4())
        payload["causation_id"] = original_event_id or dead_letter.event_id
        payload["producer"] = f"dlq-replay:{args.initiated_by}"[:64]
        producer = build_producer(settings, client_id="dlq-replay")
        await producer.start()
        try:
            await producer.send_and_wait(
                dead_letter.payload.original_topic,
                key=(dead_letter.payload.original_key or "").encode(),
                value=json.dumps(payload, separators=(",", ":")).encode(),
                headers=[
                    ("event_id", payload["event_id"].encode()),
                    ("schema_version", str(payload.get("schema_version", "")).encode()),
                    ("causation_id", str(payload["causation_id"]).encode()),
                    ("replayed_by", args.initiated_by.encode()),
                ],
            )
        finally:
            await producer.stop()
        print(f"replayed as {payload['event_id']}")
    finally:
        await consumer.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--partition", type=int, required=True)
    parser.add_argument("--offset", type=int, required=True)
    parser.add_argument("--replay", action="store_true")
    parser.add_argument("--initiated-by")
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
