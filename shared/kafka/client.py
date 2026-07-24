from __future__ import annotations

from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from shared.config.settings import Settings


def security_kwargs(settings: Settings) -> dict[str, Any]:
    values: dict[str, Any] = {
        "security_protocol": settings.kafka_security_protocol,
    }
    if settings.kafka_sasl_mechanism:
        values.update(
            sasl_mechanism=settings.kafka_sasl_mechanism,
            sasl_plain_username=settings.kafka_sasl_username,
            sasl_plain_password=settings.kafka_sasl_password,
        )
    return values


def build_producer(settings: Settings, *, client_id: str) -> AIOKafkaProducer:
    return AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        client_id=client_id,
        enable_idempotence=True,
        acks="all",
        compression_type="gzip",
        request_timeout_ms=settings.kafka_request_timeout_ms,
        max_request_size=settings.kafka_max_message_bytes,
        max_batch_size=256 * 1024,
        **security_kwargs(settings),
    )


def build_consumer(
    settings: Settings,
    *,
    topic: str,
    group_id: str,
    client_id: str,
) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        client_id=client_id,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=settings.kafka_max_poll_records,
        max_partition_fetch_bytes=settings.kafka_max_message_bytes,
        fetch_max_bytes=settings.kafka_max_message_bytes,
        max_poll_interval_ms=int(max(settings.inference_timeout_seconds * 2_000, 60_000)),
        request_timeout_ms=settings.kafka_request_timeout_ms,
        **security_kwargs(settings),
    )
