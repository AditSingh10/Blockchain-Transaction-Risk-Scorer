from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from uuid import uuid4

import structlog
from opentelemetry import trace
from prometheus_client import Counter, Gauge, Histogram
from redis.asyncio import Redis

from shared.config.settings import Settings
from shared.observability.tracing import extract_traceparent

log = structlog.get_logger()
tracer = trace.get_tracer(__name__)
active_clients = Gauge("api_gateway_websocket_active_clients", "Connected WebSocket clients")
events_sent = Counter("api_gateway_websocket_events_sent_total", "WebSocket events sent")
slow_disconnects = Counter(
    "api_gateway_websocket_slow_client_disconnects_total",
    "Clients disconnected because their bounded queue filled",
)
dropped_messages = Counter(
    "api_gateway_websocket_dropped_messages_total",
    "Messages dropped with a slow-client disconnect",
)
redis_stream_lag = Histogram(
    "api_gateway_redis_stream_lag_seconds",
    "Time from Redis Stream append to gateway observation",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.27, 0.5, 1, 5, 30),
)
redis_tail_errors = Counter(
    "api_gateway_redis_stream_errors_total",
    "Redis Stream tail failures",
)


def stream_id_tuple(stream_id: str) -> tuple[int, int]:
    milliseconds, sequence = stream_id.split("-", maxsplit=1)
    return int(milliseconds), int(sequence)


@dataclass(eq=False)
class StreamClient:
    queue_size: int
    client_id: str = field(default_factory=lambda: str(uuid4()))
    queue: asyncio.Queue = field(init=False)
    closed: asyncio.Event = field(default_factory=asyncio.Event)

    def __post_init__(self) -> None:
        self.queue = asyncio.Queue(maxsize=self.queue_size)


class RedisStreamBroadcaster:
    """Each gateway replica tails the full Redis Stream for its local clients."""

    def __init__(self, redis: Redis, settings: Settings):
        self.redis = redis
        self.settings = settings
        self.clients: set[StreamClient] = set()
        self.stop_event = asyncio.Event()
        self.task: asyncio.Task | None = None
        self.cursor = "$"

    async def start(self) -> None:
        self.task = asyncio.create_task(self._tail())

    async def stop(self) -> None:
        self.stop_event.set()
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        for client in tuple(self.clients):
            client.closed.set()
        self.clients.clear()
        active_clients.set(0)

    def register(self) -> StreamClient:
        if len(self.clients) >= self.settings.websocket_max_clients:
            raise RuntimeError("gateway WebSocket capacity reached")
        client = StreamClient(self.settings.websocket_client_queue_size)
        self.clients.add(client)
        active_clients.set(len(self.clients))
        return client

    def unregister(self, client: StreamClient) -> None:
        self.clients.discard(client)
        client.closed.set()
        active_clients.set(len(self.clients))

    async def replay_after(self, last_event_id: str) -> list[tuple[str, dict]]:
        rows = await self.redis.xrange(
            self.settings.redis_stream_key,
            min=f"({last_event_id}",
            max="+",
            count=self.settings.redis_replay_limit,
        )
        return [self._decode(stream_id, fields) for stream_id, fields in rows]

    async def replay_recent(self) -> list[tuple[str, dict]]:
        rows = await self.redis.xrevrange(
            self.settings.redis_stream_key,
            max="+",
            min="-",
            count=self.settings.redis_replay_limit,
        )
        return [self._decode(stream_id, fields) for stream_id, fields in reversed(rows)]

    @staticmethod
    def _decode(stream_id, fields) -> tuple[str, dict]:
        decoded_id = stream_id.decode() if isinstance(stream_id, bytes) else str(stream_id)
        raw = fields.get(b"event") or fields.get("event")
        if isinstance(raw, bytes):
            raw = raw.decode()
        message = json.loads(raw)
        message["stream_id"] = decoded_id
        return decoded_id, message

    async def _tail(self) -> None:
        consecutive_failures = 0
        while not self.stop_event.is_set():
            try:
                rows = await self.redis.xread(
                    {self.settings.redis_stream_key: self.cursor},
                    count=256,
                    block=1_000,
                )
                for _, entries in rows:
                    for stream_id, fields in entries:
                        decoded_id, message = self._decode(stream_id, fields)
                        self.cursor = decoded_id
                        redis_stream_lag.observe(
                            max(time.time() - stream_id_tuple(decoded_id)[0] / 1_000, 0)
                        )
                        self._broadcast(decoded_id, message)
                consecutive_failures = 0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                consecutive_failures += 1
                redis_tail_errors.inc()
                if consecutive_failures == 1 or consecutive_failures % 12 == 0:
                    log.warning(
                        "redis_stream_tail_failed",
                        attempt=consecutive_failures,
                        exception=type(exc).__name__,
                        error=str(exc)[:256],
                    )
                exponent = min(consecutive_failures - 1, 4)
                await asyncio.sleep(min(0.5 * 2**exponent, 5))

    def _broadcast(self, stream_id: str, message: dict) -> None:
        context = extract_traceparent(message.get("traceparent"))
        with tracer.start_as_current_span("redis_stream_fanout", context=context):
            for client in tuple(self.clients):
                try:
                    client.queue.put_nowait((stream_id, message))
                except asyncio.QueueFull:
                    dropped_messages.inc()
                    slow_disconnects.inc()
                    client.closed.set()
                    self.clients.discard(client)
        active_clients.set(len(self.clients))
