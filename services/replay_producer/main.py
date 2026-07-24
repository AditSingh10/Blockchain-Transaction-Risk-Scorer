from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import structlog
from opentelemetry import trace
from prometheus_client import Counter, Gauge, Histogram
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from shared.config.settings import Settings
from shared.contracts.events import (
    GraphBatchObservedV1,
    GraphBatchPayloadV1,
    GraphEdgeV1,
    GraphNodeV1,
)
from shared.contracts.ids import deterministic_event_id
from shared.database.models import StreamCheckpoint
from shared.database.queries import fetch_replay_control, update_replay_control
from shared.database.session import Database
from shared.kafka.client import build_producer
from shared.observability.tracing import current_traceparent, inject_kafka_trace
from shared.runtime import initialize_service

log = structlog.get_logger()
tracer = trace.get_tracer(__name__)
produced_total = Counter("replay_producer_events_total", "Graph batches produced", ["outcome"])
transactions_produced_total = Counter(
    "replay_producer_transactions_total",
    "Transactions carried by acknowledged graph batches",
)
current_time_step = Gauge("replay_producer_current_time_step", "Current replay time step")
configured_rate = Gauge(
    "replay_producer_events_per_second",
    "Configured transaction replay rate",
)
source_to_kafka = Histogram(
    "replay_producer_source_to_kafka_seconds",
    "Time from event creation to Kafka acknowledgement",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.27, 0.5, 1, 2),
)


class EllipticDataset:
    def __init__(self, root: Path):
        dataset = root / "elliptic_bitcoin_dataset"
        self.nodes = pd.read_csv(dataset / "elliptic_txs_features.csv", header=None)
        self.nodes = self.nodes.rename(columns={0: "txId", 1: "time_step"})
        self.edges = pd.read_csv(dataset / "elliptic_txs_edgelist.csv")
        self.time_steps = sorted(int(value) for value in self.nodes["time_step"].unique())

    def batch(self, time_step: int) -> tuple[list[GraphNodeV1], list[GraphEdgeV1]]:
        rows = self.nodes[self.nodes["time_step"] == time_step]
        nodes = [
            GraphNodeV1(
                tx_id=str(int(row["txId"])),
                time_step=time_step,
                features=[
                    float(value) for value in row.drop(["txId", "time_step"]).values.tolist()
                ],
            )
            for _, row in rows.iterrows()
        ]
        current_ids = {int(node.tx_id) for node in nodes}
        # Preserve the original simulator's graph semantics: an edge enters the
        # buffer only when both endpoint transactions belong to this timestep.
        edge_rows = self.edges[
            self.edges["txId1"].isin(current_ids) & self.edges["txId2"].isin(current_ids)
        ]
        edges = [
            GraphEdgeV1(
                source_tx_id=str(int(row["txId1"])),
                destination_tx_id=str(int(row["txId2"])),
            )
            for _, row in edge_rows.iterrows()
            if int(row["txId1"]) != int(row["txId2"])
        ]
        return nodes, edges


def pacing_delay_seconds(transaction_count: int, transactions_per_second: float) -> float:
    """Return the delay that makes a graph batch honor a transaction-level rate."""

    return transaction_count / transactions_per_second


async def producer_checkpoint(
    database: Database,
    stream_name: str,
) -> tuple[int, int]:
    async with database.session() as session:
        row = (
            await session.execute(
                select(
                    StreamCheckpoint.last_completed_time_step,
                    StreamCheckpoint.source_offset,
                ).where(StreamCheckpoint.stream_name == f"{stream_name}:producer")
            )
        ).one_or_none()
        if row is None:
            return 0, -1
        return int(row.last_completed_time_step), int(row.source_offset)


async def save_producer_checkpoint(
    database: Database,
    *,
    stream_name: str,
    time_step: int,
    source_offset: int,
) -> None:
    async with database.session() as session, session.begin():
        statement = pg_insert(StreamCheckpoint).values(
            stream_name=f"{stream_name}:producer",
            last_completed_time_step=time_step,
            source_offset=source_offset,
        )
        await session.execute(
            statement.on_conflict_do_update(
                index_elements=["stream_name"],
                set_={
                    "last_completed_time_step": statement.excluded.last_completed_time_step,
                    "source_offset": statement.excluded.source_offset,
                    "updated_at": func.now(),
                },
            )
        )


async def run() -> None:
    settings = Settings(service_name="replay-producer")
    stop = initialize_service(settings)
    database = Database(settings)
    producer = build_producer(settings, client_id=settings.service_name)
    dataset = EllipticDataset(settings.elliptic_data_dir)
    await producer.start()
    try:
        last_step, last_source_offset = await producer_checkpoint(
            database,
            settings.stream_name,
        )
        remaining = [step for step in dataset.time_steps if step > last_step]
        for source_offset, time_step in enumerate(
            remaining,
            start=last_source_offset + 1,
        ):
            if stop.is_set():
                break
            while True:
                async with database.session() as session, session.begin():
                    status, rate = await fetch_replay_control(
                        session,
                        stream_name=settings.stream_name,
                        default_rate=settings.replay_events_per_second,
                    )
                configured_rate.set(rate)
                if status != "paused":
                    break
                try:
                    await asyncio.wait_for(stop.wait(), timeout=0.5)
                except TimeoutError:
                    pass
                if stop.is_set():
                    break
            if stop.is_set():
                break

            nodes, edges = dataset.batch(time_step)
            with tracer.start_as_current_span("produce_graph_batch") as span:
                event_id = deterministic_event_id(
                    "risk.graph-batch.observed.v1",
                    settings.stream_name,
                    time_step,
                )
                span_context = span.get_span_context()
                trace_id = (
                    f"{span_context.trace_id:032x}"
                    if span_context.is_valid
                    else event_id.replace("-", "")
                )
                event_time = datetime(2020, 1, 1, tzinfo=UTC) + timedelta(minutes=time_step)
                event = GraphBatchObservedV1(
                    event_id=event_id,
                    occurred_at=event_time,
                    producer=settings.service_name,
                    trace_id=trace_id,
                    traceparent=current_traceparent(),
                    correlation_id=f"{settings.stream_name}:{time_step}",
                    payload=GraphBatchPayloadV1(
                        stream_name=settings.stream_name,
                        time_step=time_step,
                        sequence=time_step,
                        source_offset=source_offset,
                        feature_schema_version=settings.feature_schema_version,
                        nodes=nodes,
                        edges=edges,
                    ),
                )
                started = asyncio.get_running_loop().time()
                try:
                    await producer.send_and_wait(
                        settings.graph_batch_topic,
                        key=settings.stream_name.encode(),
                        value=event.model_dump_json().encode(),
                        headers=inject_kafka_trace(event.kafka_headers()),
                    )
                    await save_producer_checkpoint(
                        database,
                        stream_name=settings.stream_name,
                        time_step=time_step,
                        source_offset=source_offset,
                    )
                    produced_total.labels("success").inc()
                    transactions_produced_total.inc(len(nodes))
                    current_time_step.set(time_step)
                    source_to_kafka.observe(asyncio.get_running_loop().time() - started)
                    log.info(
                        "graph_batch_produced",
                        event_id=event_id,
                        time_step=time_step,
                        nodes=len(nodes),
                        edges=len(edges),
                    )
                except Exception:
                    produced_total.labels("failure").inc()
                    raise
            await asyncio.sleep(pacing_delay_seconds(len(nodes), rate))
        if not stop.is_set():
            async with database.session() as session, session.begin():
                await update_replay_control(
                    session,
                    stream_name=settings.stream_name,
                    status="completed",
                )
            await stop.wait()
    finally:
        await producer.stop()
        await database.dispose()


if __name__ == "__main__":
    asyncio.run(run())
