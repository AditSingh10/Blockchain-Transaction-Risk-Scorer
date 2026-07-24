from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Select, and_, func, or_, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from shared.contracts.events import (
    GraphBatchObservedV1,
    ScoringCompletedV1,
    ScoringRequestedPayloadV1,
    ScoringRequestedV1,
)
from shared.contracts.ids import deterministic_event_id
from shared.database.models import (
    ConsumerInbox,
    OutboxEvent,
    ReplayControl,
    RiskScore,
    StreamCheckpoint,
    TransactionEdge,
    TransactionNode,
)
from shared.observability.tracing import current_traceparent


def canonical_edge(source: str, destination: str) -> tuple[str, str]:
    if source == destination:
        raise ValueError("self edges are not supported")
    # Elliptic edges are directed. The primary key prevents duplicates without
    # changing the source/destination semantics.
    return source, destination


async def claim_inbox_event(
    session: AsyncSession,
    *,
    event_id: str,
    consumer_name: str,
) -> bool:
    statement = (
        pg_insert(ConsumerInbox)
        .values(event_id=event_id, consumer_name=consumer_name)
        .on_conflict_do_nothing(index_elements=["event_id", "consumer_name"])
        .returning(ConsumerInbox.event_id)
    )
    result = await session.execute(statement)
    return result.scalar_one_or_none() is not None


async def materialize_graph_batch(
    session: AsyncSession,
    *,
    event: GraphBatchObservedV1,
    scoring_topic: str,
    consumer_name: str,
) -> dict[str, int | bool]:
    """Materialize one batch and its scoring outbox in the caller's transaction."""

    if not await claim_inbox_event(
        session,
        event_id=event.event_id,
        consumer_name=consumer_name,
    ):
        return {"duplicate": True, "nodes": 0, "edges": 0, "outbox": 0}

    payload = event.payload
    observed_at = event.occurred_at
    ingested_at = event.produced_at
    node_rows = [
        {
            "tx_id": node.tx_id,
            "time_step": node.time_step,
            "features": node.features,
            "feature_schema_version": payload.feature_schema_version,
            "observed_at": observed_at,
            "ingested_at": ingested_at,
            "graph_watermark": payload.time_step,
        }
        for node in payload.nodes
    ]
    if node_rows:
        insert_nodes = pg_insert(TransactionNode).values(node_rows)
        await session.execute(
            insert_nodes.on_conflict_do_update(
                index_elements=["tx_id"],
                set_={
                    "time_step": insert_nodes.excluded.time_step,
                    "features": insert_nodes.excluded.features,
                    "feature_schema_version": insert_nodes.excluded.feature_schema_version,
                    "observed_at": insert_nodes.excluded.observed_at,
                    "ingested_at": insert_nodes.excluded.ingested_at,
                    "graph_watermark": insert_nodes.excluded.graph_watermark,
                    "updated_at": func.now(),
                },
                where=insert_nodes.excluded.graph_watermark >= TransactionNode.graph_watermark,
            )
        )

    edge_rows: list[dict[str, str]] = []
    seen_edges: set[tuple[str, str]] = set()
    for edge in payload.edges:
        normalized = canonical_edge(edge.source_tx_id, edge.destination_tx_id)
        if normalized in seen_edges:
            continue
        seen_edges.add(normalized)
        edge_rows.append({"source_tx_id": normalized[0], "destination_tx_id": normalized[1]})
    if edge_rows:
        await session.execute(
            pg_insert(TransactionEdge)
            .values(edge_rows)
            .on_conflict_do_nothing(index_elements=["source_tx_id", "destination_tx_id"])
        )

    checkpoint = pg_insert(StreamCheckpoint).values(
        stream_name=payload.stream_name,
        last_completed_time_step=payload.time_step,
        source_offset=payload.source_offset,
    )
    await session.execute(
        checkpoint.on_conflict_do_update(
            index_elements=["stream_name"],
            set_={
                "last_completed_time_step": checkpoint.excluded.last_completed_time_step,
                "source_offset": checkpoint.excluded.source_offset,
                "updated_at": func.now(),
            },
            where=checkpoint.excluded.last_completed_time_step
            >= StreamCheckpoint.last_completed_time_step,
        )
    )

    outbox_rows: list[dict[str, Any]] = []
    for node in payload.nodes:
        request_id = deterministic_event_id(
            "risk.scoring.requested.v1",
            event.event_id,
            node.tx_id,
            payload.time_step,
        )
        request = ScoringRequestedV1(
            event_id=request_id,
            occurred_at=event.occurred_at,
            produced_at=datetime.now(UTC),
            producer="graph-materializer",
            trace_id=event.trace_id,
            traceparent=current_traceparent() or event.traceparent,
            correlation_id=event.correlation_id,
            causation_id=event.event_id,
            payload=ScoringRequestedPayloadV1(
                tx_id=node.tx_id,
                graph_watermark=payload.time_step,
                source_time_step=payload.time_step,
                event_sequence=payload.sequence,
                feature_schema_version=payload.feature_schema_version,
                source_ingested_at=event.produced_at,
                requested_at=datetime.now(UTC),
            ),
        )
        outbox_rows.append(
            {
                "outbox_id": request_id,
                "aggregate_id": node.tx_id,
                "topic": scoring_topic,
                "event_key": node.tx_id,
                "payload": request.model_dump(mode="json"),
                "schema_version": request.schema_version,
            }
        )
    if outbox_rows:
        await session.execute(
            pg_insert(OutboxEvent)
            .values(outbox_rows)
            .on_conflict_do_nothing(index_elements=["outbox_id"])
        )

    return {
        "duplicate": False,
        "nodes": len(node_rows),
        "edges": len(edge_rows),
        "outbox": len(outbox_rows),
    }


async def project_scoring_result(
    session: AsyncSession,
    *,
    event: ScoringCompletedV1,
    consumer_name: str,
    redis_outbox_topic: str,
) -> bool:
    """Persist a canonical score and durable Redis publication intent."""

    if not await claim_inbox_event(
        session,
        event_id=event.event_id,
        consumer_name=consumer_name,
    ):
        return False

    payload = event.payload
    score_insert = pg_insert(RiskScore).values(
        tx_id=payload.tx_id,
        model_version=payload.model_version,
        model_checksum=payload.model_checksum,
        model_deployed_at=payload.model_deployed_at,
        feature_schema_version=payload.feature_schema_version,
        illicit_probability=payload.illicit_probability,
        queue_delay_ms=payload.queue_delay_ms,
        inference_latency_ms=payload.inference_latency_ms,
        end_to_end_latency_ms=payload.end_to_end_latency_ms,
        graph_watermark=payload.graph_watermark,
        source_event_id=event.event_id,
        scored_at=payload.scored_at,
    )
    canonical_result = await session.execute(
        score_insert.on_conflict_do_update(
            index_elements=["tx_id", "model_version"],
            set_={
                "model_checksum": score_insert.excluded.model_checksum,
                "model_deployed_at": score_insert.excluded.model_deployed_at,
                "feature_schema_version": score_insert.excluded.feature_schema_version,
                "illicit_probability": score_insert.excluded.illicit_probability,
                "queue_delay_ms": score_insert.excluded.queue_delay_ms,
                "inference_latency_ms": score_insert.excluded.inference_latency_ms,
                "end_to_end_latency_ms": score_insert.excluded.end_to_end_latency_ms,
                "graph_watermark": score_insert.excluded.graph_watermark,
                "source_event_id": score_insert.excluded.source_event_id,
                "scored_at": score_insert.excluded.scored_at,
            },
            where=and_(
                score_insert.excluded.graph_watermark >= RiskScore.graph_watermark,
                score_insert.excluded.scored_at >= RiskScore.scored_at,
            ),
        ).returning(RiskScore.source_event_id)
    )
    if canonical_result.scalar_one_or_none() is None:
        return False
    redis_outbox_id = f"redis:{event.event_id}"
    await session.execute(
        pg_insert(OutboxEvent)
        .values(
            outbox_id=redis_outbox_id,
            aggregate_id=payload.tx_id,
            topic=redis_outbox_topic,
            event_key=payload.tx_id,
            payload=event.model_dump(mode="json"),
            schema_version=event.schema_version,
        )
        .on_conflict_do_nothing(index_elements=["outbox_id"])
    )
    return True


async def fetch_graph_watermark(session: AsyncSession, stream_name: str) -> int:
    value = await session.scalar(
        select(StreamCheckpoint.last_completed_time_step).where(
            StreamCheckpoint.stream_name == stream_name
        )
    )
    return int(value or 0)


async def fetch_replay_control(
    session: AsyncSession,
    *,
    stream_name: str,
    default_rate: float,
) -> tuple[str, float]:
    statement = (
        pg_insert(ReplayControl)
        .values(
            stream_name=stream_name,
            status="running",
            events_per_second=default_rate,
        )
        .on_conflict_do_nothing(index_elements=["stream_name"])
    )
    await session.execute(statement)
    row = (
        await session.execute(
            select(ReplayControl.status, ReplayControl.events_per_second).where(
                ReplayControl.stream_name == stream_name
            )
        )
    ).one()
    return str(row.status), float(row.events_per_second)


async def update_replay_control(
    session: AsyncSession,
    *,
    stream_name: str,
    status: str | None = None,
    events_per_second: float | None = None,
) -> None:
    insert_values: dict[str, Any] = {
        "stream_name": stream_name,
        "status": status or "running",
        "events_per_second": events_per_second or 20.0,
    }
    values: dict[str, Any] = {"updated_at": func.now()}
    if status is not None:
        values["status"] = status
    if events_per_second is not None:
        values["events_per_second"] = events_per_second
    statement = pg_insert(ReplayControl).values(**insert_values)
    await session.execute(
        statement.on_conflict_do_update(
            index_elements=["stream_name"],
            set_=values,
        )
    )


async def get_transaction_entity(
    session: AsyncSession,
    *,
    tx_id: str,
    model_version: str | None = None,
    query_timeout_ms: int | None = None,
) -> dict[str, Any] | None:
    if query_timeout_ms is not None:
        await session.execute(text(f"SET LOCAL statement_timeout = {int(query_timeout_ms)}"))
    node = await session.get(TransactionNode, tx_id)
    if node is None:
        return None
    score_query: Select[Any] = select(RiskScore).where(RiskScore.tx_id == tx_id)
    if model_version:
        score_query = score_query.where(RiskScore.model_version == model_version)
    score_query = score_query.order_by(RiskScore.scored_at.desc()).limit(1)
    score = (await session.execute(score_query)).scalar_one_or_none()
    edge_rows = (
        await session.execute(
            select(
                TransactionEdge.source_tx_id,
                TransactionEdge.destination_tx_id,
            )
            .where(
                or_(
                    TransactionEdge.source_tx_id == tx_id,
                    TransactionEdge.destination_tx_id == tx_id,
                )
            )
            .order_by(
                TransactionEdge.source_tx_id,
                TransactionEdge.destination_tx_id,
            )
        )
    ).all()
    neighbors = sorted(
        {destination if source == tx_id else source for source, destination in edge_rows}
    )
    return {
        "tx_id": node.tx_id,
        "time_step": node.time_step,
        "features": node.features,
        "graph_watermark": node.graph_watermark,
        "risk_score": float(score.illicit_probability) if score else None,
        "model_version": score.model_version if score else None,
        "model_deployed_at": score.model_deployed_at if score else None,
        "inference_latency_ms": float(score.inference_latency_ms) if score else None,
        "end_to_end_latency_ms": float(score.end_to_end_latency_ms) if score else None,
        "persisted_at": score.persisted_at if score else None,
        "neighbors": neighbors,
        "cached": True,
    }


async def get_bounded_subgraph(
    session: AsyncSession,
    *,
    center_tx_id: str,
    hops: int,
    max_nodes: int,
    max_edges: int,
    query_timeout_ms: int,
    model_version: str | None = None,
) -> dict[str, Any] | None:
    """Retrieve an undirected bounded neighborhood with deterministic ordering."""

    await session.execute(text(f"SET LOCAL statement_timeout = {int(query_timeout_ms)}"))
    center_exists = await session.scalar(
        select(TransactionNode.tx_id).where(TransactionNode.tx_id == center_tx_id)
    )
    if center_exists is None:
        return None

    visited = {center_tx_id}
    frontier = {center_tx_id}
    discovered_edges: set[tuple[str, str]] = set()
    truncated = False

    for _ in range(hops):
        if not frontier:
            break
        rows = (
            await session.execute(
                select(
                    TransactionEdge.source_tx_id,
                    TransactionEdge.destination_tx_id,
                )
                .where(
                    or_(
                        TransactionEdge.source_tx_id.in_(sorted(frontier)),
                        TransactionEdge.destination_tx_id.in_(sorted(frontier)),
                    )
                )
                .order_by(
                    TransactionEdge.source_tx_id,
                    TransactionEdge.destination_tx_id,
                )
                .limit(max_edges + 1)
            )
        ).all()
        if len(rows) > max_edges:
            truncated = True
            rows = rows[:max_edges]

        next_frontier: set[str] = set()
        for source, destination in rows:
            discovered_edges.add((source, destination))
            for candidate in (source, destination):
                if candidate not in visited:
                    if len(visited) >= max_nodes:
                        truncated = True
                        continue
                    visited.add(candidate)
                    next_frontier.add(candidate)
        frontier = next_frontier

    ordered_ids = sorted(visited)
    nodes = (
        (
            await session.execute(
                select(TransactionNode)
                .where(TransactionNode.tx_id.in_(ordered_ids))
                .order_by(TransactionNode.tx_id)
            )
        )
        .scalars()
        .all()
    )
    score_query: Select[Any] = select(RiskScore).where(RiskScore.tx_id.in_(ordered_ids))
    if model_version:
        score_query = score_query.where(RiskScore.model_version == model_version)
    scores = (await session.execute(score_query)).scalars().all()
    score_map: dict[str, RiskScore] = {}
    for score in sorted(scores, key=lambda item: item.scored_at, reverse=True):
        score_map.setdefault(score.tx_id, score)

    bounded_edges = sorted(
        edge for edge in discovered_edges if edge[0] in visited and edge[1] in visited
    )
    if len(bounded_edges) > max_edges:
        bounded_edges = bounded_edges[:max_edges]
        truncated = True

    return {
        "center": center_tx_id,
        "nodes": [
            {
                "txId": node.tx_id,
                "features": node.features,
                "time_step": node.time_step,
                "risk_score": (
                    float(score_map[node.tx_id].illicit_probability)
                    if node.tx_id in score_map
                    else 0.0
                ),
            }
            for node in nodes
        ],
        "edges": bounded_edges,
        "graph_watermark": max((node.graph_watermark for node in nodes), default=0),
        "truncated": truncated,
    }


def subgraph_cache_key(
    *,
    tx_id: str,
    hops: int,
    max_nodes: int,
    max_edges: int,
    graph_watermark: int,
    model_version: str,
) -> str:
    return (
        f"risk:subgraph:v1:{tx_id}:{hops}:{max_nodes}:{max_edges}:"
        f"{graph_watermark}:{model_version}"
    )
