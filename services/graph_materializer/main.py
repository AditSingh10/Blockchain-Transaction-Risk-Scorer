from __future__ import annotations

import asyncio
import time

import structlog
from aiokafka import TopicPartition
from aiokafka.structs import OffsetAndMetadata
from opentelemetry import trace
from prometheus_client import Counter, Histogram
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError

from shared.config.settings import Settings
from shared.contracts.events import GraphBatchObservedV1
from shared.database.queries import materialize_graph_batch
from shared.database.session import Database
from shared.kafka.client import build_consumer, build_producer
from shared.kafka.dlq import publish_dead_letter, sanitize_error
from shared.kafka.metrics import observe_consumer_lag
from shared.observability.tracing import extract_kafka_context
from shared.runtime import initialize_service

log = structlog.get_logger()
tracer = trace.get_tracer(__name__)
events_total = Counter("graph_materializer_events_total", "Graph events", ["outcome"])
nodes_total = Counter("graph_materializer_nodes_upserted_total", "Nodes offered for upsert")
edges_total = Counter("graph_materializer_edges_upserted_total", "Edges offered for upsert")
outbox_total = Counter("graph_materializer_outbox_rows_total", "Scoring outbox rows created")
transaction_seconds = Histogram(
    "graph_materializer_database_transaction_seconds",
    "Graph transaction duration",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.27, 0.5, 1, 2, 5),
)


async def commit_record(consumer, record) -> None:
    partition = TopicPartition(record.topic, record.partition)
    await consumer.commit({partition: OffsetAndMetadata(record.offset + 1, "")})


async def run() -> None:
    settings = Settings(service_name="graph-materializer")
    stop = initialize_service(settings)
    database = Database(settings)
    consumer = build_consumer(
        settings,
        topic=settings.graph_batch_topic,
        group_id=settings.graph_materializer_group,
        client_id=settings.service_name,
    )
    dlq_producer = build_producer(settings, client_id=f"{settings.service_name}-dlq")
    await consumer.start()
    await dlq_producer.start()
    consecutive_failures = 0
    try:
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
                with tracer.start_as_current_span(
                    "materialize_graph_batch",
                    context=context,
                ):
                    event = GraphBatchObservedV1.model_validate_json(record.value)
                    started = time.perf_counter()
                    async with database.session() as session, session.begin():
                        outcome = await materialize_graph_batch(
                            session,
                            event=event,
                            scoring_topic=settings.scoring_request_topic,
                            consumer_name=settings.graph_materializer_group,
                        )
                    transaction_seconds.observe(time.perf_counter() - started)
                    await commit_record(consumer, record)
                    await observe_consumer_lag(
                        consumer,
                        settings.graph_materializer_group,
                    )
                    if outcome["duplicate"]:
                        events_total.labels("duplicate").inc()
                    else:
                        events_total.labels("success").inc()
                        nodes_total.inc(int(outcome["nodes"]))
                        edges_total.inc(int(outcome["edges"]))
                        outbox_total.inc(int(outcome["outbox"]))
                    log.info(
                        "graph_batch_materialized",
                        event_id=event.event_id,
                        time_step=event.payload.time_step,
                        topic=record.topic,
                        partition=record.partition,
                        offset=record.offset,
                        **outcome,
                    )
                    consecutive_failures = 0
            except ValidationError as exc:
                await publish_dead_letter(
                    producer=dlq_producer,
                    settings=settings,
                    record=record,
                    stage="graph_contract_validation",
                    error=exc,
                    attempt_count=1,
                )
                await commit_record(consumer, record)
                events_total.labels("dead_letter").inc()
                consecutive_failures = 0
            except (IntegrityError, ValueError) as exc:
                await publish_dead_letter(
                    producer=dlq_producer,
                    settings=settings,
                    record=record,
                    stage="graph_database_constraint",
                    error=exc,
                    attempt_count=1,
                )
                await commit_record(consumer, record)
                events_total.labels("dead_letter").inc()
                consecutive_failures = 0
            except Exception as exc:
                consecutive_failures += 1
                events_total.labels("failure").inc()
                log.error(
                    "graph_materialization_failed",
                    topic=record.topic,
                    partition=record.partition,
                    offset=record.offset,
                    exception=type(exc).__name__,
                    error=sanitize_error(exc),
                )
                if consecutive_failures >= settings.retry_max_attempts:
                    raise RuntimeError(
                        "graph materializer exhausted its bounded retry budget"
                    ) from exc
                await asyncio.sleep(0.5)
    finally:
        await consumer.stop()
        await dlq_producer.stop()
        await database.dispose()


if __name__ == "__main__":
    asyncio.run(run())
