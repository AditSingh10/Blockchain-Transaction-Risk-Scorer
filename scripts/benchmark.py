#!/usr/bin/env python3
"""Measure real pipeline and query latency without synthesizing results."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import platform
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import asyncpg
import psutil
import redis.asyncio as redis
import websockets

from shared.config.settings import Settings
from shared.contracts.events import GraphBatchObservedV1, GraphBatchPayloadV1, GraphNodeV1
from shared.contracts.ids import deterministic_event_id
from shared.database.queries import get_bounded_subgraph
from shared.database.session import Database
from shared.kafka.client import build_producer


def distribution(values: list[float]) -> dict[str, float | int | None]:
    ordered = sorted(values)
    if not ordered:
        return {"samples": 0, "p50": None, "p95": None, "p99": None, "max": None}

    def percentile(value: float) -> float:
        index = min(math.ceil(len(ordered) * value) - 1, len(ordered) - 1)
        return round(ordered[index], 3)

    return {
        "samples": len(ordered),
        "p50": percentile(0.50),
        "p95": percentile(0.95),
        "p99": percentile(0.99),
        "max": round(ordered[-1], 3),
    }


def throughput(count: int, started, ended) -> float | None:
    if count < 2 or started is None or ended is None or ended <= started:
        return None
    return round((count - 1) / (ended - started).total_seconds(), 3)


async def query_benchmark(
    settings: Settings,
    tx_id: str,
    *,
    hops: int,
    iterations: int,
) -> list[float]:
    database = Database(settings)
    samples: list[float] = []
    try:
        for _ in range(iterations):
            started = time.perf_counter()
            async with database.session() as session, session.begin():
                result = await get_bounded_subgraph(
                    session,
                    center_tx_id=tx_id,
                    hops=hops,
                    max_nodes=settings.graph_max_nodes,
                    max_edges=settings.graph_max_edges,
                    query_timeout_ms=settings.query_timeout_ms,
                    model_version=settings.model_version,
                )
            if result is None:
                raise RuntimeError(f"benchmark transaction {tx_id} disappeared")
            samples.append((time.perf_counter() - started) * 1_000)
    finally:
        await database.dispose()
    return samples


async def websocket_samples(
    url: str,
    *,
    cursor: str,
    expected_tx_ids: set[str],
    duration: float,
    ready: asyncio.Event,
) -> dict[str, list[float]]:
    redis_to_websocket: list[float] = []
    ingest_to_websocket: list[float] = []
    seen: set[str] = set()
    deadline = time.monotonic() + duration
    separator = "&" if "?" in url else "?"
    try:
        async with websockets.connect(
            f"{url}{separator}last_event_id={cursor}",
            open_timeout=3,
        ) as websocket:
            while time.monotonic() < deadline:
                try:
                    raw = await asyncio.wait_for(
                        websocket.recv(),
                        timeout=min(deadline - time.monotonic(), 1),
                    )
                except TimeoutError:
                    continue
                message = json.loads(raw)
                if message.get("type") == "connected":
                    ready.set()
                    continue
                data = message.get("data", {})
                if (
                    message.get("type") == "transaction"
                    and data.get("delivery_mode") == "live"
                    and data.get("tx_id") in expected_tx_ids
                    and data.get("redis_to_websocket_latency_ms") is not None
                ):
                    tx_id = str(data["tx_id"])
                    if tx_id in seen:
                        continue
                    seen.add(tx_id)
                    redis_to_websocket.append(float(data["redis_to_websocket_latency_ms"]))
                    if data.get("end_to_end_latency_ms") is not None:
                        ingest_to_websocket.append(float(data["end_to_end_latency_ms"]))
                    if seen == expected_tx_ids:
                        break
    except (OSError, websockets.WebSocketException):
        pass
    finally:
        ready.set()
    return {
        "redis_to_websocket": redis_to_websocket,
        "ingest_to_websocket": ingest_to_websocket,
    }


async def publish_benchmark_workload(
    *,
    settings: Settings,
    connection: asyncpg.Connection,
    run_id: str,
    tx_ids: list[str],
    rate: float,
) -> dict[str, Any]:
    template = await connection.fetchval(
        """
        SELECT features
        FROM transactions
        WHERE jsonb_array_length(features) = 165
        ORDER BY created_at, tx_id
        LIMIT 1
        """
    )
    if template is None:
        raise RuntimeError("benchmark requires one materialized 165-feature transaction")
    if isinstance(template, str):
        template = json.loads(template)
    features = [float(value) for value in template]
    graph_watermark = int(
        await connection.fetchval("SELECT COALESCE(max(graph_watermark), 0) FROM transactions") or 0
    )
    source_offset_value = await connection.fetchval(
        """
        SELECT COALESCE(max(source_offset), -1)
        FROM stream_checkpoints
        WHERE stream_name = $1
        """,
        settings.stream_name,
    )
    source_offset = int(source_offset_value if source_offset_value is not None else -1)
    producer = build_producer(settings, client_id=f"benchmark-{run_id}")
    acknowledgement_ms: list[float] = []
    first_started: float | None = None
    last_acknowledged: float | None = None
    loop = asyncio.get_running_loop()
    interval = 1 / rate
    await producer.start()
    try:
        for index, tx_id in enumerate(tx_ids, start=1):
            scheduled_at = loop.time()
            produced_at = datetime.now(UTC)
            time_step = graph_watermark + index
            event = GraphBatchObservedV1(
                event_id=deterministic_event_id(
                    "risk.graph-batch.observed.v1",
                    run_id,
                    index,
                ),
                occurred_at=produced_at,
                produced_at=produced_at,
                producer="benchmark-harness",
                trace_id=uuid4().hex,
                correlation_id=f"benchmark:{run_id}",
                payload=GraphBatchPayloadV1(
                    stream_name=settings.stream_name,
                    time_step=time_step,
                    sequence=time_step,
                    source_offset=source_offset + index,
                    feature_schema_version=settings.feature_schema_version,
                    nodes=[
                        GraphNodeV1(
                            tx_id=tx_id,
                            time_step=time_step,
                            features=features,
                        )
                    ],
                ),
            )
            send_started = loop.time()
            first_started = first_started or send_started
            await producer.send_and_wait(
                settings.graph_batch_topic,
                key=settings.stream_name.encode(),
                value=event.model_dump_json().encode(),
                headers=event.kafka_headers(),
            )
            last_acknowledged = loop.time()
            acknowledgement_ms.append((last_acknowledged - send_started) * 1_000)
            remaining = interval - (loop.time() - scheduled_at)
            if remaining > 0 and index < len(tx_ids):
                await asyncio.sleep(remaining)
    finally:
        await producer.stop()
    duration = (
        last_acknowledged - first_started
        if first_started is not None and last_acknowledged is not None
        else 0
    )
    return {
        "acknowledgement_ms": acknowledgement_ms,
        "throughput_per_second": (
            round((len(tx_ids) - 1) / duration, 3) if duration > 0 and len(tx_ids) > 1 else None
        ),
    }


async def wait_for_scores(
    connection: asyncpg.Connection,
    tx_ids: list[str],
    *,
    timeout_seconds: float,
) -> list[asyncpg.Record]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        rows = await connection.fetch(
            """
            SELECT tx_id, queue_delay_ms, inference_latency_ms,
                   end_to_end_latency_ms, scored_at, persisted_at,
                   extract(epoch FROM (persisted_at - scored_at)) * 1000
                     AS persistence_latency_ms
            FROM risk_scores
            WHERE tx_id = ANY($1::text[])
            ORDER BY scored_at, tx_id
            """,
            tx_ids,
        )
        if len(rows) == len(tx_ids):
            return list(rows)
        await asyncio.sleep(0.025)
    return list(rows)


async def benchmark(args: argparse.Namespace) -> dict[str, Any]:
    benchmark_started = time.perf_counter()
    settings = Settings(
        postgres_dsn=args.postgres_dsn,
        redis_url=args.redis_url,
    )
    connection = await asyncpg.connect(
        args.postgres_dsn.replace("postgresql+asyncpg://", "postgresql://", 1)
    )
    redis_client = redis.from_url(args.redis_url, decode_responses=True)
    try:
        baseline = await connection.fetchrow(
            """
            SELECT
              (SELECT count(*) FROM transactions) AS nodes,
              (SELECT count(*) FROM transaction_edges) AS edges,
              count(*) AS scores
            FROM risk_scores
            """
        )
        if not baseline or baseline["scores"] == 0:
            raise RuntimeError("benchmark requires at least one real canonical score")
        query_tx_id = await connection.fetchval(
            """
            SELECT t.tx_id
            FROM transactions t
            LEFT JOIN transaction_edges e
              ON e.source_tx_id = t.tx_id OR e.destination_tx_id = t.tx_id
            GROUP BY t.tx_id
            ORDER BY count(e.source_tx_id) DESC, t.tx_id
            LIMIT 1
            """
        )
        if query_tx_id is None:
            raise RuntimeError("benchmark requires a queryable transaction")

        run_id = uuid4().hex[:12]
        tx_ids = [f"bench-{run_id}-{index:04d}" for index in range(1, args.load_samples + 1)]
        expected_tx_ids = set(tx_ids)
        latest = await redis_client.xrevrange(
            settings.redis_stream_key,
            max="+",
            min="-",
            count=1,
        )
        cursor = str(latest[0][0]) if latest else "0-0"
        websocket_ready = asyncio.Event()
        websocket_task = asyncio.create_task(
            websocket_samples(
                args.websocket_url,
                cursor=cursor,
                expected_tx_ids=expected_tx_ids,
                duration=args.websocket_duration,
                ready=websocket_ready,
            )
        )
        await asyncio.wait_for(websocket_ready.wait(), timeout=5)
        kafka_ingestion = await publish_benchmark_workload(
            settings=settings,
            connection=connection,
            run_id=run_id,
            tx_ids=tx_ids,
            rate=args.load_rate,
        )
        rows = await wait_for_scores(
            connection,
            tx_ids,
            timeout_seconds=args.pipeline_timeout,
        )
        websocket = await websocket_task

        one_hop, two_hop = await asyncio.gather(
            query_benchmark(
                settings,
                str(query_tx_id),
                hops=1,
                iterations=args.query_iterations,
            ),
            query_benchmark(
                settings,
                str(query_tx_id),
                hops=2,
                iterations=args.query_iterations,
            ),
        )
        redis_rows = await redis_client.xrange(
            settings.redis_stream_key,
            min=f"({cursor}",
            max="+",
            count=10_000,
        )
        redis_publication: list[float] = []
        ingest_to_redis: list[float] = []
        redis_tx_ids: set[str] = set()
        for _, fields in redis_rows:
            event = json.loads(fields["event"])
            data = event.get("data", {})
            tx_id = str(data.get("tx_id", ""))
            if tx_id not in expected_tx_ids or tx_id in redis_tx_ids:
                continue
            redis_tx_ids.add(tx_id)
            if data.get("redis_publication_latency_ms") is not None:
                redis_publication.append(float(data["redis_publication_latency_ms"]))
            if data.get("ingest_to_redis_ms") is not None:
                ingest_to_redis.append(float(data["ingest_to_redis_ms"]))

        run_summary = await connection.fetchrow(
            """
            SELECT
              min(t.created_at) AS first_materialized,
              max(t.created_at) AS last_materialized,
              min(rs.scored_at) AS first_score,
              max(rs.scored_at) AS last_score,
              min(rs.persisted_at) AS first_persisted,
              max(rs.persisted_at) AS last_persisted
            FROM transactions t
            JOIN risk_scores rs ON rs.tx_id = t.tx_id
            WHERE t.tx_id = ANY($1::text[])
            """,
            tx_ids,
        )
        error_count = int(
            await connection.fetchval(
                """
                SELECT count(*)
                FROM outbox_events
                WHERE aggregate_id = ANY($1::text[])
                  AND last_error IS NOT NULL
                """,
                tx_ids,
            )
            or 0
        )
        completed = len(rows)
        missing_scores = len(tx_ids) - completed
        missing_redis = len(tx_ids) - len(redis_tx_ids)
        websocket_end_to_end = websocket["ingest_to_websocket"]
        missing_websocket = len(tx_ids) - len(websocket_end_to_end)
        ingest_to_redis_distribution = distribution(ingest_to_redis)
        ingest_to_websocket_distribution = distribution(websocket_end_to_end)
        target_actual = ingest_to_websocket_distribution["p95"]
        sample_complete = not (missing_scores or missing_redis or missing_websocket)
        return {
            "benchmark_run_id": run_id,
            "measured_at_epoch_ms": round(time.time() * 1_000),
            "test_duration_seconds": round(
                time.perf_counter() - benchmark_started,
                3,
            ),
            "hardware": {
                "platform": platform.platform(),
                "machine": platform.machine(),
                "cpu_logical": psutil.cpu_count(logical=True),
                "memory_bytes": psutil.virtual_memory().total,
            },
            "configuration": {
                "worker_count": args.worker_count,
                "baseline_dataset_nodes": int(baseline["nodes"]),
                "baseline_dataset_edges": int(baseline["edges"]),
                "baseline_canonical_scores": int(baseline["scores"]),
                "benchmark_fixture": "deterministic 165-feature graph events derived from fixture",
                "benchmark_samples": args.load_samples,
                "input_rate_per_second": args.load_rate,
                "scoring_request_partitions": settings.scoring_request_partitions,
                "scoring_result_partitions": settings.scoring_result_partitions,
                "inference_batch_size": settings.inference_batch_size,
                "batch_wait_ms": settings.inference_batch_wait_ms,
                "outbox_poll_interval_ms": settings.outbox_poll_interval_ms,
                "query_iterations": args.query_iterations,
                "websocket_observation_seconds": args.websocket_duration,
            },
            "throughput_per_second": {
                "canonical_scores": throughput(
                    completed,
                    run_summary["first_score"] if run_summary else None,
                    run_summary["last_score"] if run_summary else None,
                ),
                "result_persistence": throughput(
                    completed,
                    run_summary["first_persisted"] if run_summary else None,
                    run_summary["last_persisted"] if run_summary else None,
                ),
                "graph_materialization_nodes": throughput(
                    completed,
                    run_summary["first_materialized"] if run_summary else None,
                    run_summary["last_materialized"] if run_summary else None,
                ),
                "kafka_ingestion": kafka_ingestion["throughput_per_second"],
            },
            "latency_ms": {
                "kafka_produce_acknowledgement": distribution(
                    kafka_ingestion["acknowledgement_ms"]
                ),
                "postgres_one_hop": distribution(one_hop),
                "postgres_two_hop": distribution(two_hop),
                "kafka_queue_delay": distribution([float(row["queue_delay_ms"]) for row in rows]),
                "model_inference": distribution(
                    [float(row["inference_latency_ms"]) for row in rows]
                ),
                "scoring_ingest_to_result": distribution(
                    [float(row["end_to_end_latency_ms"]) for row in rows]
                ),
                "result_persistence": distribution(
                    [float(row["persistence_latency_ms"]) for row in rows]
                ),
                "redis_publication": distribution(redis_publication),
                "ingest_to_redis": ingest_to_redis_distribution,
                "redis_to_websocket_live": distribution(websocket["redis_to_websocket"]),
                "ingest_to_websocket": ingest_to_websocket_distribution,
            },
            "outcome": {
                "requested": len(tx_ids),
                "canonical_scores_observed": completed,
                "redis_events_observed": len(redis_tx_ids),
                "websocket_events_observed": len(websocket_end_to_end),
                "missing_scores": missing_scores,
                "missing_redis_events": missing_redis,
                "missing_websocket_events": missing_websocket,
                "outbox_errors": error_count,
                "complete": sample_complete and error_count == 0,
            },
            "target_evaluation": {
                "metric": "ingest_to_websocket p95",
                "target_ms": 270,
                "actual_ms": target_actual,
                "met": (
                    sample_complete
                    and error_count == 0
                    and isinstance(target_actual, float | int)
                    and target_actual < 270
                ),
                "scope": (
                    "Benchmark event creation through Kafka acknowledgement, graph "
                    "materialization, outbox relay, GNN scoring, result persistence, "
                    "Redis publication, and live FastAPI WebSocket delivery."
                ),
            },
        }
    finally:
        await redis_client.aclose()  # type: ignore[attr-defined]
        await connection.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--postgres-dsn",
        default="postgresql+asyncpg://risk:risk@localhost:5432/risk",
    )
    parser.add_argument("--redis-url", default="redis://localhost:6379/0")
    parser.add_argument("--websocket-url", default="ws://localhost:8000/api/v1/ws")
    parser.add_argument("--worker-count", type=int, default=1)
    parser.add_argument("--query-iterations", type=int, default=50)
    parser.add_argument("--websocket-duration", type=float, default=15)
    parser.add_argument("--pipeline-timeout", type=float, default=30)
    parser.add_argument("--load-samples", type=int, default=30)
    parser.add_argument("--load-rate", type=float, default=10)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--require-target",
        action="store_true",
        help="exit non-zero unless the complete ingest-to-WebSocket p95 is below 270 ms",
    )
    args = parser.parse_args()
    if args.load_samples < 2:
        parser.error("--load-samples must be at least 2")
    if args.load_rate <= 0:
        parser.error("--load-rate must be greater than 0")
    if args.query_iterations < 1:
        parser.error("--query-iterations must be at least 1")
    if args.websocket_duration <= 0:
        parser.error("--websocket-duration must be greater than 0")
    if args.pipeline_timeout <= 0:
        parser.error("--pipeline-timeout must be greater than 0")
    if args.worker_count < 1:
        parser.error("--worker-count must be at least 1")
    result = asyncio.run(benchmark(args))
    rendered = json.dumps(result, indent=2)
    print(rendered)
    if args.output:
        args.output.write_text(rendered + "\n")
    if args.require_target and not result["target_evaluation"]["met"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
