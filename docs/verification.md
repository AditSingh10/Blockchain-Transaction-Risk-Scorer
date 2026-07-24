# Runtime verification

This page records observed local results for the distributed pipeline. It is
evidence for the architecture claims, not a production SLA or a substitute for
measurement on deployment hardware.

## Environment

Verified on 2026-07-23 PDT with:

- macOS 26.5.2 on arm64
- 12 logical CPUs and 24 GiB memory
- Docker Compose with Kafka 7.7.1 in KRaft mode, PostgreSQL 16.6, Redis 7.4.1,
  two FastAPI gateways, and three GNN inference workers
- six scoring-request partitions and six scoring-result partitions
- deterministic 165-feature Elliptic-shaped fixture
- unchanged committed `gat-resnet-elliptic-v1` model and scaler

The complete environment started with:

```bash
docker compose up --build -d --scale inference-worker=3
```

Kafka, PostgreSQL, Redis, both gateways, the Nginx gateway load balancer, the
frontend, Prometheus, Tempo, and the OpenTelemetry Collector all reached their
ready or healthy state.

## End-to-end data path

The verifier observed one canonical prediction in PostgreSQL, the corresponding
bounded Redis Stream, a successful REST entity lookup, and a WebSocket event
with a Redis cursor:

```bash
python -m scripts.verify_pipeline --timeout 60
```

Observed result:

```json
{
  "status": "verified",
  "tx_id": "1010",
  "model_version": "gat-resnet-elliptic-v1",
  "redis_stream_length": 12,
  "websocket_stream_id": "1784860515810-0"
}
```

This exercises the real graph-batch Kafka topic, graph materializer,
transactional outbox, scoring-request topic, model worker, scoring-result
topic, result projector, PostgreSQL, Redis Stream, FastAPI, and WebSocket path.

## Failure and idempotency checks

The live integration and failure suite passed:

```text
tests/integration tests/failure: 4 passed
```

The observed invariants included:

- redelivering a graph event left one node/edge representation and one inbox
  record;
- projecting the same scoring result twice left one canonical score and one
  durable Redis-publication outbox row;
- two simultaneous WebSocket clients were routed to different gateway
  instances and received the same event ID and Redis Stream ID;
- an outbox retry reused its deterministic downstream event ID.

Two additional live failure windows were exercised:

1. An inference worker was stopped, a scoring request was accepted by Kafka, no
   result existed while the worker was down, and restarting the worker produced
   deterministic result
   `fac3bac1-fa6c-5b84-9dd9-13927f32a300`.
2. A published scoring outbox row was marked unpublished to reproduce
   publish-then-crash-before-database-update. The relay published it again, the
   projector reported a duplicate, and the database score count and Redis
   Stream length did not increase. Repeating the window for all 12 fixture
   scoring rows produced the same invariant.

Both API gateway replicas and their load balancer were restarted. The verifier
then read the existing PostgreSQL score and replayed a stored Redis Stream event
using `last_event_id=0-0`, confirming that gateway restart does not erase
canonical or replayable state.

## Latency benchmark

The final latency run created 30 isolated, deterministic graph events at 10
events/sec and followed only those transaction IDs through the full live path:

```bash
python -m scripts.benchmark \
  --worker-count 3 \
  --load-samples 30 \
  --load-rate 10 \
  --require-target \
  --output benchmark.json
```

All 30 events reached PostgreSQL, Redis, and the live WebSocket. There were no
missing scores, missing events, or outbox errors.

| Measurement | p50 | p95 | p99 | Maximum |
|---|---:|---:|---:|---:|
| Kafka produce acknowledgement | 1.66 ms | 2.86 ms | 3.66 ms | 3.66 ms |
| Kafka/request queue delay | 21.32 ms | 79.27 ms | 79.60 ms | 79.60 ms |
| GNN inference | 4.12 ms | 20.76 ms | 21.73 ms | 21.73 ms |
| Scoring ingest to result | 29.99 ms | 105.68 ms | 145.99 ms | 145.99 ms |
| Result persistence | 2.06 ms | 20.40 ms | 46.95 ms | 46.95 ms |
| Ingest to Redis | 55.37 ms | 164.84 ms | 256.52 ms | 256.52 ms |
| Redis to live WebSocket | 0.95 ms | 1.51 ms | 1.55 ms | 1.55 ms |
| Event creation to live WebSocket | 55.97 ms | **165.41 ms** | 259.27 ms | 259.27 ms |

Measured canonical-score throughput was 10.26 events/sec for the configured
10-events/sec input. The p95 full-path result was 165.41 ms, below the 270 ms
target for this stated load and hardware.

## Horizontal inference scaling

The same 300-event burst was published at a requested 500 events/sec after
starting from zero consumer lag.

| Worker replicas | Completed | Canonical scores/sec | Full-path p95 |
|---:|---:|---:|---:|
| 1 | 300/300 | 70.65 | 3,569.30 ms |
| 3 | 300/300 | 236.35 | 771.72 ms |

Three workers increased measured scoring capacity by **3.34×** without missing
canonical scores, Redis events, or WebSocket events. Kafka assigned two of the
six request partitions to each worker. The 500-events/sec input deliberately
exceeded even the three-worker capacity, so Kafka lag and latency rose while
memory remained bounded; the latency target is not claimed for this overload
case.

The initial one-worker burst exposed a 1-second Kafka poll interval that
artificially limited bounded queue refills. Reducing the typed default to 100 ms
raised measured one-worker capacity from roughly 8 to 70.65 scores/sec. The
final comparisons above use the corrected setting on both sides.

## Interpretation and limits

These results directly support a horizontally scalable Kafka-backed GNN
pipeline and a measured sub-270 ms full path at 10 events/sec on the stated
machine. They do not prove a cloud production SLA, high availability of the
single-broker local dependencies, or performance over the entire private
Elliptic dataset. Production sizing must rerun the committed harness on target
hardware with the intended dataset, partition count, worker count, and traffic
distribution.
