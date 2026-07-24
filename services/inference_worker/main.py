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
from sqlalchemy.exc import SQLAlchemyError

from shared.config.settings import Settings
from shared.contracts.events import (
    ScoringCompletedPayloadV1,
    ScoringCompletedV1,
    ScoringRequestedV1,
)
from shared.contracts.ids import deterministic_event_id
from shared.database.queries import get_bounded_subgraph
from shared.database.session import Database
from shared.kafka.client import build_consumer, build_producer
from shared.kafka.dlq import publish_dead_letter, sanitize_error
from shared.kafka.metrics import observe_consumer_lag
from shared.kafka.retry import (
    MissingGraphStateError,
    ModelInferenceError,
    PermanentEventError,
    retry_delay_seconds,
)
from shared.model.runtime import ModelRuntime
from shared.observability.tracing import (
    current_traceparent,
    extract_kafka_context,
    inject_kafka_trace,
)
from shared.runtime import initialize_service

log = structlog.get_logger()
tracer = trace.get_tracer(__name__)
requests_total = Counter("inference_worker_requests_total", "Scoring requests", ["outcome"])
retry_total = Counter("inference_worker_retry_total", "Inference retry attempts")
dlq_total = Counter("inference_worker_dlq_total", "Inference requests dead-lettered")
in_flight = Gauge("inference_worker_in_flight", "Current worker in-flight requests")
queue_depth = Gauge("inference_worker_queue_depth", "Bounded local request queue depth")
model_load_seconds = Gauge("inference_worker_model_load_seconds", "Model startup load duration")
worker_ready = Gauge(
    "inference_worker_ready",
    "One after this process loaded the model and joined Kafka",
)
inference_seconds = Histogram(
    "inference_worker_inference_seconds",
    "Model inference duration",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.27, 0.5, 1, 2, 5),
)
queue_delay_seconds = Histogram(
    "inference_worker_queue_delay_seconds",
    "Kafka/request queue delay",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.27, 0.5, 1, 5, 30),
)
end_to_end_seconds = Histogram(
    "inference_worker_end_to_end_seconds",
    "Ingest-to-result-publication latency",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.27, 0.5, 1, 2, 5, 30),
)
batch_size_histogram = Histogram(
    "inference_worker_batch_size",
    "Kafka poll batch size",
    buckets=(1, 2, 4, 8, 16, 32, 64, 128),
)
batch_wait_seconds = Histogram(
    "inference_worker_batch_wait_seconds",
    "Micro-batch wait duration; zero while graph-safe batching is disabled",
    buckets=(0, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1),
)


async def commit_record(consumer, record) -> None:
    partition = TopicPartition(record.topic, record.partition)
    await consumer.commit({partition: OffsetAndMetadata(record.offset + 1, "")})


async def score_and_publish(
    *,
    event: ScoringRequestedV1,
    database: Database,
    model: ModelRuntime,
    producer,
    settings: Settings,
) -> ScoringCompletedV1:
    if event.payload.feature_schema_version != model.feature_schema_version:
        raise PermanentEventError(
            f"feature schema {event.payload.feature_schema_version} does not match "
            f"worker schema {model.feature_schema_version}"
        )
    async with database.session() as session, session.begin():
        subgraph = await get_bounded_subgraph(
            session,
            center_tx_id=event.payload.tx_id,
            hops=settings.graph_max_hops,
            max_nodes=settings.graph_max_nodes,
            max_edges=settings.graph_max_edges,
            query_timeout_ms=settings.query_timeout_ms,
            model_version=settings.model_version,
        )
    if subgraph is None:
        raise MissingGraphStateError(
            f"transaction {event.payload.tx_id} is absent at graph watermark "
            f"{event.payload.graph_watermark}"
        )
    if subgraph["graph_watermark"] < event.payload.graph_watermark:
        raise MissingGraphStateError(
            f"graph watermark {subgraph['graph_watermark']} is behind request "
            f"{event.payload.graph_watermark}"
        )
    started_at = datetime.now(UTC)
    inference_started = time.perf_counter()
    try:
        probability = await asyncio.wait_for(
            asyncio.to_thread(model.predict, subgraph),
            timeout=settings.inference_timeout_seconds,
        )
    except TimeoutError:
        raise
    except Exception as exc:
        raise ModelInferenceError("model inference failed") from exc
    inference_ms = (time.perf_counter() - inference_started) * 1_000
    scored_at = datetime.now(UTC)
    queue_ms = max((started_at - event.payload.requested_at).total_seconds() * 1_000, 0)
    end_to_end_ms = max(
        (scored_at - event.payload.source_ingested_at).total_seconds() * 1_000,
        0,
    )
    result_id = deterministic_event_id(
        "risk.scoring.completed.v1",
        event.event_id,
        model.model_version,
    )
    result = ScoringCompletedV1(
        event_id=result_id,
        occurred_at=event.occurred_at,
        produced_at=scored_at,
        producer=settings.service_name,
        trace_id=event.trace_id,
        traceparent=current_traceparent() or event.traceparent,
        correlation_id=event.correlation_id,
        causation_id=event.event_id,
        payload=ScoringCompletedPayloadV1(
            tx_id=event.payload.tx_id,
            illicit_probability=probability,
            model_version=model.model_version,
            model_checksum=model.model_checksum,
            model_deployed_at=model.deployment_timestamp,
            feature_schema_version=model.feature_schema_version,
            graph_watermark=event.payload.graph_watermark,
            source_time_step=event.payload.source_time_step,
            event_sequence=event.payload.event_sequence,
            source_ingested_at=event.payload.source_ingested_at,
            queue_delay_ms=queue_ms,
            inference_latency_ms=inference_ms,
            end_to_end_latency_ms=end_to_end_ms,
            scored_at=scored_at,
        ),
    )
    await producer.send_and_wait(
        settings.scoring_result_topic,
        key=event.payload.tx_id.encode(),
        value=result.model_dump_json().encode(),
        headers=inject_kafka_trace(result.kafka_headers()),
    )
    inference_seconds.observe(inference_ms / 1_000)
    queue_delay_seconds.observe(queue_ms / 1_000)
    end_to_end_seconds.observe(end_to_end_ms / 1_000)
    return result


async def process_record(
    *,
    record,
    consumer,
    producer,
    database: Database,
    model: ModelRuntime,
    settings: Settings,
) -> None:
    try:
        event = ScoringRequestedV1.model_validate_json(record.value)
    except ValidationError as exc:
        await publish_dead_letter(
            producer=producer,
            settings=settings,
            record=record,
            stage="scoring_contract_validation",
            error=exc,
            attempt_count=1,
        )
        await commit_record(consumer, record)
        requests_total.labels("dead_letter").inc()
        dlq_total.inc()
        return

    context = extract_kafka_context(record.headers)
    with tracer.start_as_current_span("score_transaction", context=context) as span:
        span.set_attribute("risk.tx_id", event.payload.tx_id)
        span.set_attribute("messaging.kafka.partition", record.partition)
        span.set_attribute("messaging.kafka.offset", record.offset)
        last_error: Exception | None = None
        first_failure_at: datetime | None = None
        attempt_used = 1
        for attempt in range(1, settings.retry_max_attempts + 1):
            attempt_used = attempt
            try:
                result = await score_and_publish(
                    event=event,
                    database=database,
                    model=model,
                    producer=producer,
                    settings=settings,
                )
                await commit_record(consumer, record)
                await observe_consumer_lag(consumer, settings.inference_worker_group)
                requests_total.labels("success").inc()
                log.info(
                    "scoring_completed",
                    event_id=result.event_id,
                    transaction_id=event.payload.tx_id,
                    model_version=model.model_version,
                    queue_delay_ms=result.payload.queue_delay_ms,
                    inference_latency_ms=result.payload.inference_latency_ms,
                    end_to_end_latency_ms=result.payload.end_to_end_latency_ms,
                    topic=record.topic,
                    partition=record.partition,
                    offset=record.offset,
                )
                return
            except PermanentEventError as exc:
                last_error = exc
                first_failure_at = first_failure_at or datetime.now(UTC)
                break
            except Exception as exc:
                last_error = exc
                first_failure_at = first_failure_at or datetime.now(UTC)
                if attempt == settings.retry_max_attempts:
                    break
                retry_total.inc()
                await asyncio.sleep(
                    retry_delay_seconds(
                        attempt,
                        settings.retry_base_delay_ms,
                        settings.retry_max_delay_ms,
                    )
                )
        assert last_error is not None
        if isinstance(last_error, MissingGraphStateError):
            failure_stage = "missing_graph_state"
        elif isinstance(last_error, ModelInferenceError):
            failure_stage = "model_inference"
        elif isinstance(last_error, SQLAlchemyError):
            failure_stage = "database"
        elif isinstance(last_error, TimeoutError):
            failure_stage = "model_inference_timeout"
        elif isinstance(last_error, PermanentEventError):
            failure_stage = "feature_schema_validation"
        else:
            failure_stage = "scoring_poison_event"
        await publish_dead_letter(
            producer=producer,
            settings=settings,
            record=record,
            stage=failure_stage,
            error=last_error,
            attempt_count=attempt_used,
            first_failure_at=first_failure_at,
        )
        await commit_record(consumer, record)
        requests_total.labels("dead_letter").inc()
        dlq_total.inc()
        log.error(
            "scoring_dead_lettered",
            transaction_id=event.payload.tx_id,
            exception=type(last_error).__name__,
            error=sanitize_error(last_error),
        )


async def poll_into_queue(
    consumer,
    queue: asyncio.Queue,
    stop: asyncio.Event,
    settings: Settings,
) -> None:
    paused = False
    while not stop.is_set():
        if queue.full():
            assignment = consumer.assignment()
            if assignment and not paused:
                consumer.pause(*assignment)
                paused = True
            await asyncio.sleep(0.01)
            queue_depth.set(queue.qsize())
            continue
        if paused and queue.qsize() <= max(queue.maxsize // 2, 1):
            consumer.resume(*consumer.assignment())
            paused = False
        batches = await consumer.getmany(
            timeout_ms=settings.kafka_poll_timeout_ms,
            max_records=min(settings.kafka_max_poll_records, queue.maxsize - queue.qsize()),
        )
        count = sum(len(records) for records in batches.values())
        if count:
            batch_size_histogram.observe(count)
            batch_wait_seconds.observe(0)
        for records in batches.values():
            for record in records:
                await queue.put(record)
        queue_depth.set(queue.qsize())


async def drain_queue(
    consumer,
    producer,
    queue: asyncio.Queue,
    database: Database,
    model: ModelRuntime,
    settings: Settings,
) -> None:
    while True:
        record = await queue.get()
        if record is None:
            queue.task_done()
            return
        in_flight.inc()
        try:
            await process_record(
                record=record,
                consumer=consumer,
                producer=producer,
                database=database,
                model=model,
                settings=settings,
            )
        finally:
            in_flight.dec()
            queue.task_done()
            queue_depth.set(queue.qsize())


async def run() -> None:
    settings = Settings(service_name="inference-worker")
    stop = initialize_service(settings)
    database = Database(settings)
    model_started = time.perf_counter()
    model = ModelRuntime(
        model_path=settings.model_path,
        scaler_path=settings.scaler_path,
        model_version=settings.model_version,
        feature_schema_version=settings.feature_schema_version,
        deployment_timestamp=settings.model_deployment_timestamp,
    )
    model_load_seconds.set(time.perf_counter() - model_started)
    consumer = build_consumer(
        settings,
        topic=settings.scoring_request_topic,
        group_id=settings.inference_worker_group,
        client_id=settings.service_name,
    )
    producer = build_producer(settings, client_id=f"{settings.service_name}-results")
    queue: asyncio.Queue = asyncio.Queue(maxsize=settings.max_in_flight_inference)
    await consumer.start()
    await producer.start()
    worker_ready.set(1)
    poller = asyncio.create_task(poll_into_queue(consumer, queue, stop, settings))
    worker = asyncio.create_task(drain_queue(consumer, producer, queue, database, model, settings))
    stop_task = asyncio.create_task(stop.wait())
    try:
        done, _ = await asyncio.wait(
            {stop_task, poller, worker},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if stop_task in done:
            poller.cancel()
            await asyncio.gather(poller, return_exceptions=True)
            await asyncio.wait_for(
                queue.join(),
                timeout=settings.shutdown_timeout_seconds,
            )
            await queue.put(None)
            await worker
        else:
            for task in done:
                task.result()
    finally:
        stop_task.cancel()
        poller.cancel()
        worker.cancel()
        await asyncio.gather(stop_task, poller, worker, return_exceptions=True)
        worker_ready.set(0)
        await consumer.stop()
        await producer.stop()
        await database.dispose()


if __name__ == "__main__":
    asyncio.run(run())
