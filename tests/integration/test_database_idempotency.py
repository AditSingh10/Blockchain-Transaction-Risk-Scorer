from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from shared.config.settings import Settings
from shared.contracts.events import (
    GraphBatchObservedV1,
    GraphBatchPayloadV1,
    GraphEdgeV1,
    GraphNodeV1,
    ScoringCompletedPayloadV1,
    ScoringCompletedV1,
)
from shared.database.models import (
    ConsumerInbox,
    OutboxEvent,
    RiskScore,
    TransactionEdge,
    TransactionNode,
)
from shared.database.queries import (
    get_bounded_subgraph,
    materialize_graph_batch,
    project_scoring_result,
)
from shared.database.session import Database

pytestmark = pytest.mark.integration


def graph_event() -> GraphBatchObservedV1:
    suffix = uuid4().hex[:8]
    event_id = str(uuid4())
    return GraphBatchObservedV1(
        event_id=event_id,
        occurred_at=datetime.now(UTC),
        producer="integration-test",
        trace_id=uuid4().hex,
        correlation_id=suffix,
        payload=GraphBatchPayloadV1(
            stream_name=f"test-{suffix}",
            time_step=1,
            sequence=1,
            source_offset=0,
            feature_schema_version="elliptic-165-v1",
            nodes=[
                GraphNodeV1(tx_id=f"{suffix}-a", time_step=1, features=[0.1] * 165),
                GraphNodeV1(tx_id=f"{suffix}-b", time_step=1, features=[0.2] * 165),
            ],
            edges=[
                GraphEdgeV1(
                    source_tx_id=f"{suffix}-a",
                    destination_tx_id=f"{suffix}-b",
                )
            ],
        ),
    )


@pytest.mark.asyncio
async def test_commit_before_offset_redelivery_is_idempotent() -> None:
    settings = Settings()
    database = Database(settings)
    event = graph_event()
    try:
        for _ in range(2):
            async with database.session() as session, session.begin():
                await materialize_graph_batch(
                    session,
                    event=event,
                    scoring_topic=settings.scoring_request_topic,
                    consumer_name="failure-window-test",
                )
        async with database.session() as session:
            node_count = await session.scalar(
                select(func.count())
                .select_from(TransactionNode)
                .where(TransactionNode.tx_id.in_([node.tx_id for node in event.payload.nodes]))
            )
            edge_count = await session.scalar(
                select(func.count())
                .select_from(TransactionEdge)
                .where(TransactionEdge.source_tx_id == event.payload.nodes[0].tx_id)
            )
            inbox_count = await session.scalar(
                select(func.count())
                .select_from(ConsumerInbox)
                .where(ConsumerInbox.event_id == event.event_id)
            )
            outbox_count = await session.scalar(
                select(func.count())
                .select_from(OutboxEvent)
                .where(OutboxEvent.aggregate_id.in_([node.tx_id for node in event.payload.nodes]))
            )
        assert (node_count, edge_count, inbox_count, outbox_count) == (2, 1, 1, 2)
        async with database.session() as session, session.begin():
            subgraph = await get_bounded_subgraph(
                session,
                center_tx_id=event.payload.nodes[0].tx_id,
                hops=2,
                max_nodes=10,
                max_edges=10,
                query_timeout_ms=1_000,
            )
        assert subgraph is not None
        assert [node["txId"] for node in subgraph["nodes"]] == sorted(
            node.tx_id for node in event.payload.nodes
        )
        assert subgraph["edges"] == [
            (
                event.payload.nodes[0].tx_id,
                event.payload.nodes[1].tx_id,
            )
        ]
        assert subgraph["truncated"] is False
    finally:
        await database.dispose()


@pytest.mark.asyncio
async def test_duplicate_results_leave_one_canonical_score_and_publication() -> None:
    settings = Settings()
    database = Database(settings)
    graph = graph_event()
    async with database.session() as session, session.begin():
        await materialize_graph_batch(
            session,
            event=graph,
            scoring_topic=settings.scoring_request_topic,
            consumer_name="result-fixture",
        )
    tx_id = graph.payload.nodes[0].tx_id
    result = ScoringCompletedV1(
        event_id=str(uuid4()),
        occurred_at=graph.occurred_at,
        producer="integration-worker",
        trace_id=graph.trace_id,
        correlation_id=graph.correlation_id,
        causation_id=graph.event_id,
        payload=ScoringCompletedPayloadV1(
            tx_id=tx_id,
            illicit_probability=0.71,
            model_version=settings.model_version,
            model_checksum=f"sha256:{'0' * 64}",
            feature_schema_version=settings.feature_schema_version,
            graph_watermark=1,
            source_time_step=1,
            event_sequence=1,
            source_ingested_at=graph.produced_at,
            queue_delay_ms=2,
            inference_latency_ms=3,
            end_to_end_latency_ms=7,
            scored_at=datetime.now(UTC),
        ),
    )
    try:
        for _ in range(2):
            async with database.session() as session, session.begin():
                await project_scoring_result(
                    session,
                    event=result,
                    consumer_name="duplicate-result-test",
                    redis_outbox_topic=f"redis:{settings.redis_stream_key}",
                )
        async with database.session() as session:
            scores = await session.scalar(
                select(func.count())
                .select_from(RiskScore)
                .where(
                    RiskScore.tx_id == tx_id,
                    RiskScore.model_version == settings.model_version,
                )
            )
            publications = await session.scalar(
                select(func.count())
                .select_from(OutboxEvent)
                .where(OutboxEvent.outbox_id == f"redis:{result.event_id}")
            )
        assert scores == 1
        assert publications == 1
    finally:
        await database.dispose()
