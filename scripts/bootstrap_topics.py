#!/usr/bin/env python3
"""Create the fixed, versioned Kafka topics used by the pipeline."""

from __future__ import annotations

import asyncio

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from shared.config.settings import Settings
from shared.kafka.client import security_kwargs


async def bootstrap() -> None:
    settings = Settings(service_name="topic-bootstrap")
    admin = AIOKafkaAdminClient(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        client_id=settings.service_name,
        **security_kwargs(settings),
    )
    topics = [
        NewTopic(
            settings.graph_batch_topic,
            num_partitions=settings.graph_batch_partitions,
            replication_factor=1,
            topic_configs={
                "retention.ms": "604800000",
                "max.message.bytes": str(settings.kafka_max_message_bytes),
            },
        ),
        NewTopic(
            settings.scoring_request_topic,
            num_partitions=settings.scoring_request_partitions,
            replication_factor=1,
            topic_configs={
                "retention.ms": "604800000",
                "max.message.bytes": str(settings.kafka_max_message_bytes),
            },
        ),
        NewTopic(
            settings.scoring_result_topic,
            num_partitions=settings.scoring_result_partitions,
            replication_factor=1,
            topic_configs={
                "retention.ms": "604800000",
                "max.message.bytes": str(settings.kafka_max_message_bytes),
            },
        ),
        NewTopic(
            settings.scoring_dlq_topic,
            num_partitions=settings.scoring_dlq_partitions,
            replication_factor=1,
            topic_configs={
                "retention.ms": "2592000000",
                "max.message.bytes": str(settings.kafka_max_message_bytes),
            },
        ),
    ]
    await admin.start()
    try:
        try:
            await admin.create_topics(topics, validate_only=False)
        except TopicAlreadyExistsError:
            pass
        existing = await admin.list_topics()
        missing = sorted(topic.name for topic in topics if topic.name not in existing)
        if missing:
            raise RuntimeError(f"Kafka topic bootstrap incomplete; missing: {missing}")
        print("Kafka topics ready:", ", ".join(sorted(topic.name for topic in topics)))
    finally:
        await admin.close()


if __name__ == "__main__":
    asyncio.run(bootstrap())
