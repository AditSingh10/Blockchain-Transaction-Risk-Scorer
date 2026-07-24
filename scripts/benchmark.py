#!/usr/bin/env python3
"""Measure real pipeline and query latency without synthesizing results."""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import time
from pathlib import Path
from typing import Any

import asyncpg
import psutil
import redis.asyncio as redis
import websockets

from shared.config.settings import Settings
from shared.database.queries import get_bounded_subgraph
from shared.database.session import Database


def distribution(values: list[float]) -> dict[str, float | int | None]:
    ordered = sorted(values)
    if not ordered:
        return {"samples": 0, "p50": None, "p95": None, "p99": None, "max": None}

    def percentile(value: float) -> float:
        index = min(round((len(ordered) - 1) * value), len(ordered) - 1)
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
    return round(count / (ended - started).total_seconds(), 3)


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


async def websocket_samples(url: str, duration: float) -> list[float]:
    samples: list[float] = []
    deadline = time.monotonic() + duration
    try:
        async with websockets.connect(url, open_timeout=3) as websocket:
            while time.monotonic() < deadline:
                try:
                    raw = await asyncio.wait_for(
                        websocket.recv(),
                        timeout=min(deadline - time.monotonic(), 1),
                    )
                except TimeoutError:
                    continue
                message = json.loads(raw)
                data = message.get("data", {})
                if (
                    message.get("type") == "transaction"
                    and data.get("delivery_mode") == "live"
                    and data.get("redis_to_websocket_latency_ms") is not None
                ):
                    samples.append(float(data["redis_to_websocket_latency_ms"]))
    except (OSError, websockets.WebSocketException):
        return []
    return samples


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
        summary = await connection.fetchrow(
            """
            SELECT
              (SELECT count(*) FROM transactions) AS nodes,
              (SELECT count(*) FROM transaction_edges) AS edges,
              (SELECT min(created_at) FROM transactions) AS first_materialized,
              (SELECT max(created_at) FROM transactions) AS last_materialized,
              count(*) AS scores,
              min(scored_at) AS first_score,
              max(scored_at) AS last_score,
              min(persisted_at) AS first_persisted,
              max(persisted_at) AS last_persisted
            FROM risk_scores
            """
        )
        if not summary or summary["scores"] == 0:
            raise RuntimeError("benchmark requires at least one real canonical score")
        rows = await connection.fetch(
            """
            SELECT tx_id, queue_delay_ms, inference_latency_ms,
                   end_to_end_latency_ms,
                   extract(epoch FROM (persisted_at - scored_at)) * 1000
                     AS persistence_latency_ms
            FROM risk_scores
            ORDER BY scored_at
            """
        )
        tx_id = str(rows[len(rows) // 2]["tx_id"])
        one_hop, two_hop, websocket = await asyncio.gather(
            query_benchmark(settings, tx_id, hops=1, iterations=args.query_iterations),
            query_benchmark(settings, tx_id, hops=2, iterations=args.query_iterations),
            websocket_samples(args.websocket_url, args.websocket_duration),
        )
        redis_rows = await redis_client.xrange(
            settings.redis_stream_key,
            min="-",
            max="+",
            count=10_000,
        )
        redis_publication: list[float] = []
        ingest_to_redis: list[float] = []
        for _, fields in redis_rows:
            event = json.loads(fields["event"])
            data = event.get("data", {})
            if data.get("redis_publication_latency_ms") is not None:
                redis_publication.append(float(data["redis_publication_latency_ms"]))
            if data.get("ingest_to_redis_ms") is not None:
                ingest_to_redis.append(float(data["ingest_to_redis_ms"]))

        error_count = int(
            await connection.fetchval(
                "SELECT count(*) FROM outbox_events WHERE last_error IS NOT NULL"
            )
            or 0
        )
        ingest_to_redis_distribution = distribution(ingest_to_redis)
        target_actual = ingest_to_redis_distribution["p95"]
        return {
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
                "dataset_nodes": int(summary["nodes"]),
                "dataset_edges": int(summary["edges"]),
                "canonical_scores": int(summary["scores"]),
                "scoring_request_partitions": settings.scoring_request_partitions,
                "scoring_result_partitions": settings.scoring_result_partitions,
                "inference_batch_size": settings.inference_batch_size,
                "batch_wait_ms": settings.inference_batch_wait_ms,
                "query_iterations": args.query_iterations,
                "websocket_observation_seconds": args.websocket_duration,
            },
            "throughput_per_second": {
                "canonical_scores": throughput(
                    int(summary["scores"]),
                    summary["first_score"],
                    summary["last_score"],
                ),
                "result_persistence": throughput(
                    int(summary["scores"]),
                    summary["first_persisted"],
                    summary["last_persisted"],
                ),
                "graph_materialization_nodes": throughput(
                    int(summary["nodes"]),
                    summary["first_materialized"],
                    summary["last_materialized"],
                ),
                "kafka_ingestion": None,
                "note": (
                    "Kafka broker ingestion requires a concurrent counter sample; "
                    "a null value is not an estimate."
                ),
            },
            "latency_ms": {
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
                "redis_to_websocket_live": distribution(websocket),
            },
            "error_count": error_count,
            "target_evaluation": {
                "metric": "ingest_to_redis p95",
                "target_ms": 270,
                "actual_ms": target_actual,
                "met": (isinstance(target_actual, float | int) and target_actual < 270),
                "scope": (
                    "Source ingestion through bounded Redis publication. Live "
                    "Redis-to-WebSocket samples are reported separately."
                ),
            },
        }
    finally:
        await redis_client.close()
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
    parser.add_argument("--websocket-duration", type=float, default=10)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = asyncio.run(benchmark(args))
    rendered = json.dumps(result, indent=2)
    print(rendered)
    if args.output:
        args.output.write_text(rendered + "\n")


if __name__ == "__main__":
    main()
