from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from typing import Any

from aiokafka import AIOKafkaProducer

from shared.config.settings import Settings
from shared.contracts.events import DeadLetterEventV1, DeadLetterPayloadV1
from shared.contracts.ids import deterministic_event_id
from shared.observability.tracing import inject_kafka_trace


def _header_value(headers: list[tuple[str, bytes]] | None, name: str) -> str | None:
    for key, value in headers or []:
        if key == name:
            return value.decode(errors="replace")
    return None


def _safe_original_payload(raw: bytes) -> dict[str, Any] | str:
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else str(parsed)
    except Exception:
        return raw.decode(errors="replace")[:32_000]


def sanitize_error(error: Exception) -> str:
    message = str(error)
    message = re.sub(r"://[^@\s]+@", "://***@", message)
    message = re.sub(
        r"(?i)(password|secret|token|api[_-]?key)=([^&\s]+)",
        r"\1=***",
        message,
    )
    return message[:512]


async def publish_dead_letter(
    *,
    producer: AIOKafkaProducer,
    settings: Settings,
    record,
    stage: str,
    error: Exception,
    attempt_count: int,
    first_failure_at: datetime | None = None,
) -> str:
    now = datetime.now(UTC)
    original_event_id = _header_value(record.headers, "event_id")
    trace_id = _header_value(record.headers, "trace_id") or deterministic_event_id(
        "trace", record.topic, record.partition, record.offset
    ).replace("-", "")
    dlq_id = deterministic_event_id(
        "risk.dead-lettered.v1",
        record.topic,
        record.partition,
        record.offset,
        stage,
    )
    event = DeadLetterEventV1(
        event_id=dlq_id,
        occurred_at=now,
        produced_at=now,
        producer=settings.service_name,
        trace_id=trace_id,
        traceparent=_header_value(record.headers, "traceparent"),
        correlation_id=original_event_id or dlq_id,
        causation_id=original_event_id,
        payload=DeadLetterPayloadV1(
            original_topic=record.topic,
            original_partition=record.partition,
            original_offset=record.offset,
            original_key=record.key.decode(errors="replace") if record.key else None,
            original_payload=_safe_original_payload(record.value),
            original_event_id=original_event_id,
            failure_stage=stage,
            exception_class=type(error).__name__,
            sanitized_error=sanitize_error(error),
            attempt_count=attempt_count,
            first_failure_at=first_failure_at or now,
            last_failure_at=now,
        ),
    )
    await producer.send_and_wait(
        settings.scoring_dlq_topic,
        key=(record.key or dlq_id.encode()),
        value=event.model_dump_json().encode(),
        headers=inject_kafka_trace(event.kafka_headers()),
    )
    return dlq_id
