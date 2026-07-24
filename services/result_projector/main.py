from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime

import structlog
from aiokafka import TopicPartition
from aiokafka.structs import OffsetAndMetadata
from opentelemetry import trace
from prometheus_client import Counter, Gauge, Histogram
from pydantic import ValidationError
from redis.asyncio import Redis
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from shared.config.settings import Settings
from shared.contracts.events import ScoringCompletedV1
from shared.database.models import OutboxEvent
from shared.database.queries import get_transaction_entity, project_scoring_result
from shared.database.session import Database
from shared.kafka.client import build_consumer, build_producer
from shared.kafka.dlq import publish_dead_letter, sanitize_error
from shared.kafka.metrics import observe_consumer_lag
from shared.observability.tracing import extract_kafka_context, extract_traceparent
from shared.redis.stream import publish_result_to_stream
from shared.runtime import initialize_service

log = structlog.get_logger()
tracer = trace.get_tracer(__name__)
results_total = Counter("result_projector_results_total", "Scoring results", ["outcome"])
redis_failures = Counter("result_projector_redis_failures_total", "Redis publication failures")
redis_published = Counter("result_projector_redis_published_total", "Redis results published")
redis_backlog = Gauge("result_projector_redis_backlog", "Unpublished Redis outbox rows")
persistence_seconds = Histogram(
    "result_projector_persistence_seconds",
    "Result database persistence latency",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.27, 0.5, 1),
)


async def commit_record(consumer, record) -> None:
    partition = TopicPartition(record.topic, record.partition)
    await consumer.commit({partition: OffsetAndMetadata(record.offset + 1, "")})


async def consume_results(
    *,
    stop: asyncio.Event,
    consumer,
    dlq_producer,
    database: Database,
    settings: Settings,
) -> None:
    consecutive_failures = 0
    while not stop.is_set():
        try:
            record = await asyncio.wait_for(
                consumer.getone(),
                timeout=settings.kafka_poll_timeout_ms / 1_000,
            )
        except TimeoutError:
            continue
        try:
            context = extract_kafka_context(record.headers)
            with tracer.start_as_current_span("project_scoring_result", context=context):
                event = ScoringCompletedV1.model_validate_json(record.value)
                started = time.perf_counter()
                async with database.session() as session, session.begin():
                    inserted = await project_scoring_result(
                        session,
                        event=event,
                        consumer_name=settings.result_projector_group,
                        redis_outbox_topic=f"redis:{settings.redis_stream_key}",
                    )
                persistence_seconds.observe(time.perf_counter() - started)
                await commit_record(consumer, record)
                await observe_consumer_lag(consumer, settings.result_projector_group)
                results_total.labels("persisted" if inserted else "duplicate").inc()
            log.info(
                "scoring_result_projected",
                event_id=event.event_id,
                transaction_id=event.payload.tx_id,
                model_version=event.payload.model_version,
                duplicate=not inserted,
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
            )
            consecutive_failures = 0
        except ValidationError as exc:
            await publish_dead_letter(
                producer=dlq_producer,
                settings=settings,
                record=record,
                stage="result_contract_validation",
                error=exc,
                attempt_count=1,
            )
            await commit_record(consumer, record)
            results_total.labels("dead_letter").inc()
            consecutive_failures = 0
        except IntegrityError as exc:
            await publish_dead_letter(
                producer=dlq_producer,
                settings=settings,
                record=record,
                stage="result_database_constraint",
                error=exc,
                attempt_count=1,
            )
            await commit_record(consumer, record)
            results_total.labels("dead_letter").inc()
            consecutive_failures = 0
        except Exception as exc:
            consecutive_failures += 1
            results_total.labels("failure").inc()
            log.error(
                "result_projection_failed",
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                exception=type(exc).__name__,
                error=sanitize_error(exc),
            )
            if consecutive_failures >= settings.retry_max_attempts:
                raise RuntimeError("result projector exhausted its bounded retry budget") from exc
            await asyncio.sleep(0.5)


async def redis_payload(
    session,
    *,
    outbox: OutboxEvent,
    settings: Settings,
) -> dict:
    event = ScoringCompletedV1.model_validate(outbox.payload)
    entity = await get_transaction_entity(
        session,
        tx_id=event.payload.tx_id,
        model_version=event.payload.model_version,
        query_timeout_ms=settings.query_timeout_ms,
    )
    if entity is None:
        raise RuntimeError(f"transaction {event.payload.tx_id} disappeared before projection")
    features = entity["features"]
    amount = abs(float(features[2])) * 10 if len(features) > 2 else 0.0
    published_at = datetime.now(UTC)
    persisted_at = entity["persisted_at"]
    persistence_latency_ms = (
        max((persisted_at - event.payload.scored_at).total_seconds() * 1_000, 0)
        if persisted_at
        else 0.0
    )
    redis_publication_latency_ms = max(
        (published_at - event.payload.scored_at).total_seconds() * 1_000,
        0,
    )
    ingest_to_redis_ms = max(
        (published_at - event.payload.source_ingested_at).total_seconds() * 1_000,
        0,
    )
    return {
        "type": "transaction",
        "event_id": event.event_id,
        "schema_version": event.schema_version,
        "trace_id": event.trace_id,
        "traceparent": event.traceparent,
        "data": {
            "tx_id": event.payload.tx_id,
            "timestamp": int(event.payload.scored_at.timestamp() * 1_000),
            "amount": round(amount, 6),
            "illicit_probability": round(event.payload.illicit_probability, 6),
            "threshold": settings.presentation_threshold,
            "flagged": event.payload.illicit_probability >= settings.presentation_threshold,
            "inference_latency_ms": round(event.payload.inference_latency_ms, 2),
            "queue_delay_ms": round(event.payload.queue_delay_ms, 2),
            "end_to_end_latency_ms": round(event.payload.end_to_end_latency_ms, 2),
            "scoring_end_to_end_latency_ms": round(
                event.payload.end_to_end_latency_ms,
                2,
            ),
            "persistence_latency_ms": round(persistence_latency_ms, 2),
            "redis_publication_latency_ms": round(redis_publication_latency_ms, 2),
            "ingest_to_redis_ms": round(ingest_to_redis_ms, 2),
            "source_ingested_at": event.payload.source_ingested_at.isoformat(),
            "neighbors": entity["neighbors"],
            "model_version": event.payload.model_version,
            "model_checksum": event.payload.model_checksum,
            "model_deployed_at": (
                event.payload.model_deployed_at.isoformat()
                if event.payload.model_deployed_at
                else None
            ),
            "feature_schema_version": event.payload.feature_schema_version,
            "graph_watermark": event.payload.graph_watermark,
        },
    }


async def publish_redis_outbox(
    *,
    stop: asyncio.Event,
    database: Database,
    redis: Redis,
    settings: Settings,
) -> None:
    topic = f"redis:{settings.redis_stream_key}"
    consecutive_failures = 0
    while not stop.is_set():
        try:
            async with database.session() as session, session.begin():
                rows = (
                    (
                        await session.execute(
                            select(OutboxEvent)
                            .where(
                                OutboxEvent.topic == topic,
                                OutboxEvent.published_at.is_(None),
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
                    event = ScoringCompletedV1.model_validate(row.payload)
                    context = extract_traceparent(event.traceparent)
                    with tracer.start_as_current_span(
                        "publish_result_to_redis",
                        context=context,
                    ):
                        payload = await redis_payload(session, outbox=row, settings=settings)
                        stream_id = await publish_result_to_stream(
                            redis,
                            stream_key=settings.redis_stream_key,
                            max_length=settings.redis_stream_maxlen,
                            event_id=str(payload["event_id"]),
                            payload=payload,
                        )
                    row.published_at = datetime.now(UTC)
                    row.last_error = None
                    redis_published.inc()
                    log.info(
                        "result_published_to_redis",
                        event_id=payload["event_id"],
                        transaction_id=row.aggregate_id,
                        redis_stream_id=stream_id,
                    )
            async with database.session() as session:
                count = await session.scalar(
                    select(func.count())
                    .select_from(OutboxEvent)
                    .where(
                        OutboxEvent.topic == topic,
                        OutboxEvent.published_at.is_(None),
                    )
                )
            redis_backlog.set(int(count or 0))
            if not rows:
                await asyncio.sleep(settings.outbox_poll_interval_ms / 1_000)
            consecutive_failures = 0
        except Exception as exc:
            consecutive_failures += 1
            redis_failures.inc()
            log.warning(
                "redis_result_publication_failed",
                exception=type(exc).__name__,
                error=sanitize_error(exc),
            )
            if consecutive_failures >= settings.retry_max_attempts:
                raise RuntimeError("Redis relay exhausted its bounded retry budget") from exc
            await asyncio.sleep(0.5)


async def run() -> None:
    settings = Settings(service_name="result-projector")
    stop = initialize_service(settings)
    database = Database(settings)
    redis = Redis.from_url(
        settings.redis_url,
        decode_responses=False,
        socket_timeout=settings.redis_socket_timeout_seconds,
        socket_connect_timeout=settings.redis_socket_timeout_seconds,
    )
    consumer = build_consumer(
        settings,
        topic=settings.scoring_result_topic,
        group_id=settings.result_projector_group,
        client_id=settings.service_name,
    )
    dlq_producer = build_producer(settings, client_id=f"{settings.service_name}-dlq")
    await consumer.start()
    await dlq_producer.start()
    consumer_task = asyncio.create_task(
        consume_results(
            stop=stop,
            consumer=consumer,
            dlq_producer=dlq_producer,
            database=database,
            settings=settings,
        )
    )
    redis_task = asyncio.create_task(
        publish_redis_outbox(
            stop=stop,
            database=database,
            redis=redis,
            settings=settings,
        )
    )
    stop_task = asyncio.create_task(stop.wait())
    try:
        done, _ = await asyncio.wait(
            {stop_task, consumer_task, redis_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if stop_task not in done:
            for task in done:
                task.result()
        stop.set()
        await asyncio.gather(consumer_task, redis_task)
    finally:
        stop_task.cancel()
        consumer_task.cancel()
        redis_task.cancel()
        await asyncio.gather(consumer_task, redis_task, return_exceptions=True)
        await consumer.stop()
        await dlq_producer.stop()
        await redis.aclose()  # type: ignore[attr-defined]
        await database.dispose()


if __name__ == "__main__":
    asyncio.run(run())
