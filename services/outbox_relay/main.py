from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

import structlog
from opentelemetry import trace
from prometheus_client import Counter, Gauge, Histogram
from sqlalchemy import func, select, update

from shared.config.settings import Settings
from shared.database.models import OutboxEvent
from shared.database.session import Database
from shared.kafka.client import build_producer
from shared.kafka.retry import retry_delay_seconds
from shared.observability.tracing import extract_traceparent, inject_kafka_trace
from shared.runtime import initialize_service

log = structlog.get_logger()
tracer = trace.get_tracer(__name__)
published_total = Counter("outbox_relay_events_published_total", "Outbox events published")
failures_total = Counter("outbox_relay_publication_failures_total", "Outbox publish failures")
retry_total = Counter("outbox_relay_retry_total", "Outbox retry attempts")
backlog_gauge = Gauge("outbox_relay_backlog", "Unpublished Kafka outbox rows")
publication_seconds = Histogram(
    "outbox_relay_publication_seconds",
    "Outbox row age when published",
    buckets=(0.01, 0.05, 0.1, 0.27, 0.5, 1, 2, 5, 10, 30, 60),
)


def event_headers(payload: dict) -> list[tuple[str, bytes]]:
    values = [
        ("event_type", str(payload.get("event_type", "")).encode()),
        ("schema_version", str(payload.get("schema_version", "")).encode()),
        ("event_id", str(payload.get("event_id", "")).encode()),
        ("trace_id", str(payload.get("trace_id", "")).encode()),
        ("correlation_id", str(payload.get("correlation_id", "")).encode()),
    ]
    return inject_kafka_trace(values)


async def record_failure(database: Database, outbox_id: str, error: Exception) -> None:
    """Persist a failed attempt after the publishing transaction rolls back."""

    async with database.session() as session, session.begin():
        await session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.outbox_id == outbox_id)
            .values(
                attempt_count=OutboxEvent.attempt_count + 1,
                last_error=str(error)[:512],
            )
        )


async def update_backlog(database: Database) -> None:
    async with database.session() as session:
        count = await session.scalar(
            select(func.count())
            .select_from(OutboxEvent)
            .where(
                OutboxEvent.published_at.is_(None),
                ~OutboxEvent.topic.startswith("redis:"),
            )
        )
        backlog_gauge.set(int(count or 0))


async def run() -> None:
    settings = Settings(service_name="outbox-relay")
    stop = initialize_service(settings)
    database = Database(settings)
    producer = build_producer(settings, client_id=settings.service_name)
    await producer.start()
    attempt = 0
    failed_outbox_id: str | None = None
    try:
        while not stop.is_set():
            try:
                async with database.session() as session, session.begin():
                    rows = (
                        (
                            await session.execute(
                                select(OutboxEvent)
                                .where(
                                    OutboxEvent.published_at.is_(None),
                                    ~OutboxEvent.topic.startswith("redis:"),
                                )
                                .order_by(OutboxEvent.created_at, OutboxEvent.outbox_id)
                                .with_for_update(skip_locked=True)
                                .limit(settings.outbox_batch_size)
                            )
                        )
                        .scalars()
                        .all()
                    )
                    for row in rows:
                        failed_outbox_id = row.outbox_id
                        context = extract_traceparent(row.payload.get("traceparent"))
                        with tracer.start_as_current_span(
                            "publish_outbox_event",
                            context=context,
                        ):
                            await producer.send_and_wait(
                                row.topic,
                                key=row.event_key.encode(),
                                value=json.dumps(
                                    row.payload,
                                    separators=(",", ":"),
                                ).encode(),
                                headers=event_headers(row.payload),
                            )
                        row.published_at = datetime.now(UTC)
                        row.last_error = None
                        published_total.inc()
                        publication_seconds.observe(
                            max((row.published_at - row.created_at).total_seconds(), 0)
                        )
                        failed_outbox_id = None
                    published = len(rows)
                attempt = 0
                await update_backlog(database)
                if published == 0:
                    await asyncio.sleep(settings.outbox_poll_interval_ms / 1_000)
            except Exception as exc:
                failures_total.inc()
                retry_total.inc()
                if failed_outbox_id is not None:
                    await record_failure(database, failed_outbox_id, exc)
                    failed_outbox_id = None
                attempt = min(attempt + 1, settings.retry_max_attempts)
                delay = retry_delay_seconds(
                    attempt,
                    settings.retry_base_delay_ms,
                    settings.retry_max_delay_ms,
                )
                log.warning(
                    "outbox_publish_retry",
                    attempt=attempt,
                    delay_seconds=delay,
                    exception=type(exc).__name__,
                )
                if attempt >= settings.retry_max_attempts:
                    raise RuntimeError("outbox relay exhausted its bounded retry budget") from exc
                await asyncio.sleep(delay)
    finally:
        await producer.stop()
        await database.dispose()


if __name__ == "__main__":
    asyncio.run(run())
