from __future__ import annotations

import asyncio
import json
import re
import time
from contextlib import asynccontextmanager, suppress
from datetime import datetime
from typing import TypedDict

import structlog
from fastapi import FastAPI, HTTPException, Path, Query, Response, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from opentelemetry import trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import ValidationError
from redis.asyncio import Redis
from sqlalchemy import text

from services.api_gateway.broadcaster import (
    RedisStreamBroadcaster,
    StreamClient,
    events_sent,
    stream_id_tuple,
)
from services.api_gateway.schemas import (
    CONTROL_MESSAGE_ADAPTER,
    ReplayStatusMessage,
    SpeedMessage,
    ThresholdMessage,
)
from shared.config.settings import Settings
from shared.database.queries import (
    fetch_graph_watermark,
    fetch_replay_control,
    get_bounded_subgraph,
    get_transaction_entity,
    subgraph_cache_key,
    update_replay_control,
)
from shared.database.session import Database
from shared.kafka.dlq import sanitize_error
from shared.observability import configure_logging, configure_tracing
from shared.observability.tracing import extract_traceparent

log = structlog.get_logger()
tracer = trace.get_tracer(__name__)
rest_requests = Counter("api_gateway_rest_requests_total", "REST requests", ["route", "outcome"])
rest_latency = Histogram(
    "api_gateway_rest_latency_seconds",
    "REST handler latency",
    ["route"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.27, 0.5, 1, 2),
)
db_latency = Histogram(
    "api_gateway_database_query_seconds",
    "Database query latency",
    ["query"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.27, 0.5, 1),
)
cache_total = Counter("api_gateway_cache_total", "Subgraph cache accesses", ["outcome"])
errors_total = Counter("api_gateway_errors_total", "Gateway errors", ["stage"])

TX_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
STREAM_ID_PATTERN = re.compile(r"^\d+-\d+$")


class WebSocketState(TypedDict):
    threshold: float
    last_event_id: str | None


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings(service_name="api-gateway")
    configure_logging(settings)
    configure_tracing(settings)
    database = Database(settings)
    redis = Redis.from_url(
        settings.redis_url,
        decode_responses=False,
        socket_timeout=settings.redis_socket_timeout_seconds,
        socket_connect_timeout=settings.redis_socket_timeout_seconds,
    )
    broadcaster = RedisStreamBroadcaster(redis, settings)
    await broadcaster.start()
    app.state.settings = settings
    app.state.database = database
    app.state.redis = redis
    app.state.broadcaster = broadcaster
    try:
        yield
    finally:
        await broadcaster.stop()
        await redis.close()
        await database.dispose()


app = FastAPI(title="Risk Monitor API Gateway", version="1.0.0", lifespan=lifespan)
bootstrap_settings = Settings(service_name="api-gateway")
app.add_middleware(
    CORSMiddleware,
    allow_origins=bootstrap_settings.cors_origins,
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["Content-Type", "Last-Event-ID"],
)
FastAPIInstrumentor.instrument_app(app)


def validate_tx_id(tx_id: str) -> str:
    if not TX_ID_PATTERN.fullmatch(tx_id):
        raise HTTPException(status_code=422, detail="invalid transaction ID")
    return tx_id


async def dependencies_ready(app: FastAPI) -> tuple[bool, dict[str, bool]]:
    database_ok = False
    redis_ok = False
    try:
        async with app.state.database.session() as session:
            await asyncio.wait_for(session.execute(text("SELECT 1")), timeout=1.0)
        database_ok = True
    except Exception:
        pass
    try:
        redis_ok = bool(await asyncio.wait_for(app.state.redis.ping(), timeout=1.0))
    except Exception:
        pass
    return database_ok and redis_ok, {"postgres": database_ok, "redis": redis_ok}


@app.get("/health/live")
async def health_live() -> dict:
    return {"status": "alive"}


@app.get("/health/ready")
async def health_ready(response: Response) -> dict:
    ready, dependencies = await dependencies_ready(app)
    if not ready:
        response.status_code = 503
    return {"status": "ready" if ready else "not_ready", "dependencies": dependencies}


@app.get("/health")
async def health_legacy(response: Response) -> dict:
    ready, dependencies = await dependencies_ready(app)
    if not ready:
        response.status_code = 503
    return {
        "status": "ok" if ready else "degraded",
        "model_loaded": None,
        "scoring_ready": ready,
        "error": None if ready else "gateway dependency unavailable",
        "dependencies": dependencies,
    }


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/api/v1/entity/{tx_id}")
@app.get("/entity/{tx_id}")
async def entity(
    tx_id: str = Path(min_length=1, max_length=64),
    threshold: float = Query(default=0.9, ge=0.0, le=1.0),
) -> dict:
    started = time.perf_counter()
    tx_id = validate_tx_id(tx_id)
    settings: Settings = app.state.settings
    try:
        query_started = time.perf_counter()
        async with app.state.database.session() as session, session.begin():
            result = await get_transaction_entity(
                session,
                tx_id=tx_id,
                model_version=settings.model_version,
                query_timeout_ms=settings.query_timeout_ms,
            )
        db_latency.labels("entity").observe(time.perf_counter() - query_started)
        if result is None:
            rest_requests.labels("entity", "not_found").inc()
            raise HTTPException(status_code=404, detail=f"Transaction {tx_id} not found")
        probability = result["risk_score"]
        rest_requests.labels("entity", "success").inc()
        return {
            "tx_id": result["tx_id"],
            "risk_score": probability if probability is not None else 0.0,
            "flagged": probability is not None and probability >= threshold,
            "neighbors": result["neighbors"],
            "cached": result["cached"],
            "model_version": result["model_version"],
            "model_deployed_at": (
                result["model_deployed_at"].isoformat() if result["model_deployed_at"] else None
            ),
            "graph_watermark": result["graph_watermark"],
            "inference_latency_ms": result["inference_latency_ms"],
            "end_to_end_latency_ms": result["end_to_end_latency_ms"],
        }
    except HTTPException:
        raise
    except Exception as exc:
        errors_total.labels("entity").inc()
        rest_requests.labels("entity", "failure").inc()
        log.warning(
            "entity_query_failed",
            transaction_id=tx_id,
            exception=type(exc).__name__,
            error=sanitize_error(exc),
        )
        raise HTTPException(status_code=503, detail="serving dependency unavailable") from None
    finally:
        rest_latency.labels("entity").observe(time.perf_counter() - started)


@app.get("/api/v1/subgraph/{tx_id}")
@app.get("/subgraph/{tx_id}")
async def subgraph(
    tx_id: str = Path(min_length=1, max_length=64),
    hops: int = Query(default=2, ge=1, le=4),
    max_nodes: int | None = Query(default=None, ge=1, le=5_000),
    max_edges: int | None = Query(default=None, ge=1, le=20_000),
) -> dict:
    started = time.perf_counter()
    tx_id = validate_tx_id(tx_id)
    settings: Settings = app.state.settings
    node_limit = min(max_nodes or settings.graph_max_nodes, settings.graph_max_nodes)
    edge_limit = min(max_edges or settings.graph_max_edges, settings.graph_max_edges)
    try:
        async with app.state.database.session() as session:
            watermark = await fetch_graph_watermark(session, settings.stream_name)
        cache_key = subgraph_cache_key(
            tx_id=tx_id,
            hops=hops,
            max_nodes=node_limit,
            max_edges=edge_limit,
            graph_watermark=watermark,
            model_version=settings.model_version,
        )
        try:
            cached = await app.state.redis.get(cache_key)
        except Exception:
            cached = None
            errors_total.labels("subgraph_cache_read").inc()
        if cached:
            cache_total.labels("hit").inc()
            result = json.loads(cached)
        else:
            cache_total.labels("miss").inc()
            query_started = time.perf_counter()
            async with app.state.database.session() as session, session.begin():
                result = await get_bounded_subgraph(
                    session,
                    center_tx_id=tx_id,
                    hops=hops,
                    max_nodes=node_limit,
                    max_edges=edge_limit,
                    query_timeout_ms=settings.query_timeout_ms,
                    model_version=settings.model_version,
                )
            db_latency.labels("subgraph").observe(time.perf_counter() - query_started)
            if result is not None:
                try:
                    await app.state.redis.setex(
                        cache_key,
                        settings.redis_subgraph_ttl_seconds,
                        json.dumps(result, separators=(",", ":")),
                    )
                except Exception:
                    errors_total.labels("subgraph_cache_write").inc()
        if result is None:
            rest_requests.labels("subgraph", "not_found").inc()
            raise HTTPException(status_code=404, detail=f"Transaction {tx_id} not found")
        rest_requests.labels("subgraph", "success").inc()
        return {
            "nodes": [
                {"id": node["txId"], "risk_score": node["risk_score"]} for node in result["nodes"]
            ],
            "edges": [
                {"source": source, "target": destination} for source, destination in result["edges"]
            ],
            "center": result["center"],
            "graph_watermark": result["graph_watermark"],
            "truncated": result["truncated"],
        }
    except HTTPException:
        raise
    except Exception as exc:
        errors_total.labels("subgraph").inc()
        rest_requests.labels("subgraph", "failure").inc()
        log.warning(
            "subgraph_query_failed",
            transaction_id=tx_id,
            exception=type(exc).__name__,
            error=sanitize_error(exc),
        )
        raise HTTPException(status_code=503, detail="serving dependency unavailable") from None
    finally:
        rest_latency.labels("subgraph").observe(time.perf_counter() - started)


def personalize(message: dict, threshold: float) -> dict:
    outbound = {**message, "data": dict(message.get("data", {}))}
    probability = float(outbound["data"].get("illicit_probability", 0))
    outbound["data"]["threshold"] = threshold
    outbound["data"]["flagged"] = probability >= threshold
    return outbound


def annotate_delivery(message: dict, *, replayed: bool) -> dict:
    if message.get("type") != "transaction":
        return message
    outbound = {**message, "data": dict(message.get("data", {}))}
    now_ms = time.time() * 1_000
    stream_id = outbound.get("stream_id")
    if stream_id:
        outbound["data"]["redis_to_websocket_latency_ms"] = round(
            max(now_ms - stream_id_tuple(str(stream_id))[0], 0),
            2,
        )
    source_ingested_at = outbound["data"].get("source_ingested_at")
    if source_ingested_at:
        try:
            source_ms = datetime.fromisoformat(str(source_ingested_at)).timestamp() * 1_000
            outbound["data"]["end_to_end_latency_ms"] = round(
                max(now_ms - source_ms, 0),
                2,
            )
        except ValueError:
            pass
    outbound["data"]["delivery_mode"] = "replay" if replayed else "live"
    return outbound


async def send_message(
    websocket: WebSocket,
    message: dict,
    settings: Settings,
    *,
    replayed: bool = False,
) -> None:
    message = annotate_delivery(message, replayed=replayed)
    context = extract_traceparent(message.get("traceparent"))
    with tracer.start_as_current_span("websocket_publish", context=context):
        await asyncio.wait_for(
            websocket.send_text(json.dumps(message, separators=(",", ":"))),
            timeout=settings.websocket_send_timeout_seconds,
        )
    events_sent.inc()


async def replay_telemetry(settings: Settings) -> dict[str, str | float]:
    try:
        async with app.state.database.session() as session, session.begin():
            status, events_per_second = await fetch_replay_control(
                session,
                stream_name=settings.stream_name,
                default_rate=settings.replay_events_per_second,
            )
        return {
            "replay_status": status,
            "replay_events_per_second": events_per_second,
        }
    except Exception as exc:
        errors_total.labels("replay_telemetry").inc()
        log.warning(
            "replay_telemetry_unavailable",
            exception=type(exc).__name__,
            error=sanitize_error(exc),
        )
        return {}


async def websocket_sender(
    websocket: WebSocket,
    client: StreamClient,
    settings: Settings,
    state: WebSocketState,
) -> None:
    while not client.closed.is_set():
        try:
            stream_id, message = await asyncio.wait_for(
                client.queue.get(),
                timeout=settings.websocket_heartbeat_seconds,
            )
            if state["last_event_id"] and stream_id_tuple(stream_id) <= stream_id_tuple(
                state["last_event_id"]
            ):
                client.queue.task_done()
                continue
            await send_message(websocket, personalize(message, state["threshold"]), settings)
            state["last_event_id"] = stream_id
            client.queue.task_done()
        except TimeoutError:
            replay = await replay_telemetry(settings)
            await send_message(
                websocket,
                {
                    "type": "heartbeat",
                    "timestamp": int(time.time() * 1_000),
                    "last_event_id": state["last_event_id"],
                    **replay,
                },
                settings,
            )


async def websocket_receiver(
    websocket: WebSocket,
    settings: Settings,
    state: WebSocketState,
) -> None:
    while True:
        raw = await websocket.receive_text()
        if len(raw.encode()) > settings.websocket_max_message_bytes:
            await websocket.close(code=1009, reason="message too large")
            return
        try:
            message = CONTROL_MESSAGE_ADAPTER.validate_json(raw)
        except ValidationError:
            await send_message(
                websocket,
                {"type": "error", "message": "invalid control message"},
                settings,
            )
            continue
        if isinstance(message, ThresholdMessage):
            state["threshold"] = message.value
        elif isinstance(message, SpeedMessage):
            events_per_second = min(max(1.0 / message.interval, 0.1), 10_000)
            async with app.state.database.session() as session, session.begin():
                await update_replay_control(
                    session,
                    stream_name=settings.stream_name,
                    events_per_second=events_per_second,
                )
        elif isinstance(message, ReplayStatusMessage):
            async with app.state.database.session() as session, session.begin():
                current_status, _ = await fetch_replay_control(
                    session,
                    stream_name=settings.stream_name,
                    default_rate=settings.replay_events_per_second,
                )
                # Completion is terminal for a replay run. A late browser
                # control message must not advertise a resumed producer when
                # the durable checkpoint is already at the end of the source.
                if current_status != "completed":
                    await update_replay_control(
                        session,
                        stream_name=settings.stream_name,
                        status="paused" if message.type == "pause_replay" else "running",
                    )


async def websocket_handler(
    websocket: WebSocket,
    last_event_id: str | None,
) -> None:
    settings: Settings = app.state.settings
    broadcaster: RedisStreamBroadcaster = app.state.broadcaster
    if last_event_id and not STREAM_ID_PATTERN.fullmatch(last_event_id):
        await websocket.close(code=1008, reason="invalid last_event_id")
        return
    try:
        client = broadcaster.register()
    except RuntimeError:
        await websocket.close(code=1013, reason="gateway at capacity")
        return
    await websocket.accept()
    replay_metadata = await replay_telemetry(settings)
    await send_message(
        websocket,
        {
            "type": "connected",
            "gateway_instance": settings.instance_id,
            "heartbeat_seconds": settings.websocket_heartbeat_seconds,
            "last_event_id": last_event_id,
            **replay_metadata,
        },
        settings,
    )
    state: WebSocketState = {
        "threshold": settings.presentation_threshold,
        "last_event_id": last_event_id,
    }
    try:
        replayed_events = (
            await broadcaster.replay_after(last_event_id)
            if last_event_id
            else await broadcaster.replay_recent()
        )
        for stream_id, message in replayed_events:
            await send_message(
                websocket,
                personalize(message, state["threshold"]),
                settings,
                replayed=True,
            )
            state["last_event_id"] = stream_id
        sender = asyncio.create_task(websocket_sender(websocket, client, settings, state))
        receiver = asyncio.create_task(websocket_receiver(websocket, settings, state))
        closer = asyncio.create_task(client.closed.wait())
        done, pending = await asyncio.wait(
            {sender, receiver, closer},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        for task in done:
            with suppress(WebSocketDisconnect, asyncio.CancelledError):
                task.result()
        if client.closed.is_set():
            await websocket.close(code=1013, reason="slow client")
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        errors_total.labels("websocket").inc()
        log.warning(
            "websocket_connection_failed",
            client_id=client.client_id,
            exception=type(exc).__name__,
            error=str(exc)[:256],
        )
        with suppress(Exception):
            await websocket.close(code=1011, reason="gateway error")
    finally:
        broadcaster.unregister(client)


@app.websocket("/ws")
async def websocket_legacy(
    websocket: WebSocket,
    last_event_id: str | None = Query(default=None, max_length=64),
):
    await websocket_handler(websocket, last_event_id)


@app.websocket("/api/v1/ws")
async def websocket_v1(
    websocket: WebSocket,
    last_event_id: str | None = Query(default=None, max_length=64),
):
    await websocket_handler(websocket, last_event_id)
