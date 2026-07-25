# Crypto Fraud Detection / Risk Monitor

A production-style, event-driven Bitcoin transaction risk-scoring system using
the committed GAT-ResNet model trained on the Elliptic dataset. Kafka carries
the real graph and scoring path, PostgreSQL owns durable graph/prediction state,
independently scalable workers run the unchanged model, Redis Streams provide
bounded WebSocket replay, and stateless FastAPI gateways serve the React
investigation console.

![Risk Monitor live blockchain fraud investigation console](docs/images/risk-monitor-live-console.png)

## Architecture

```text
Elliptic replay → Kafka graph batches → graph materializer → PostgreSQL + outbox
    → Kafka scoring requests → GNN worker group → Kafka scoring results
    → result projector → PostgreSQL + Redis Stream → FastAPI → React
```

### Analyst serving path

```mermaid
flowchart TB
    U["USERS / ANALYSTS"]
    UI["REACT DASHBOARD"]
    LB["LOAD BALANCER"]
    A1["FASTAPI GATEWAY 1"]
    A2["FASTAPI GATEWAY 2"]
    S["SHARED DATA LAYER"]
    R["REDIS STREAMS<br/>live events and reconnect replay"]
    DB[("POSTGRESQL<br/>transactions, graph, scores")]

    U --> UI
    UI --> LB
    LB --> A1
    LB --> A2
    A1 --> S
    A2 --> S
    S --> DB
    S --> R

    classDef primary fill:#1473c9,color:#fff,stroke:#0c5da8,stroke-width:1px;
    classDef secondary fill:#e9eef3,color:#17212b,stroke:#c8d1da,stroke-width:1px;
    classDef storage fill:#dce8f5,color:#17212b,stroke:#1473c9,stroke-width:1px;
    class U,UI,S secondary;
    class LB,A1,A2 primary;
    class R,DB storage;
```

The load balancer may send any REST request or WebSocket connection to any
gateway. Gateways are stateless: PostgreSQL is the source of truth, and Redis
holds only bounded real-time history.

### Transaction scoring path

```mermaid
flowchart TB
    E["ELLIPTIC REPLAY PRODUCER"]
    K1["KAFKA<br/>graph batches"]
    M["GRAPH MATERIALIZER"]
    PG1[("POSTGRESQL<br/>graph + transactional outbox")]
    O["OUTBOX RELAY"]
    K2["KAFKA<br/>scoring requests"]
    W["INFERENCE WORKERS<br/>1 ... N"]
    K3["KAFKA<br/>scoring results"]
    P["RESULT PROJECTOR"]
    PG2[("POSTGRESQL<br/>canonical scores")]
    RS["REDIS STREAM<br/>bounded live replay"]
    API["FASTAPI GATEWAYS"]
    DASH["REACT DASHBOARD"]

    E --> K1
    K1 --> M
    M --> PG1
    PG1 --> O
    O --> K2
    K2 --> W
    W --> K3
    K3 --> P
    P --> PG2
    P --> RS
    PG2 --> API
    RS --> API
    API --> DASH

    classDef primary fill:#1473c9,color:#fff,stroke:#0c5da8,stroke-width:1px;
    classDef secondary fill:#e9eef3,color:#17212b,stroke:#c8d1da,stroke-width:1px;
    classDef storage fill:#dce8f5,color:#17212b,stroke:#1473c9,stroke-width:1px;
    class E,DASH secondary;
    class M,O,W,P,API primary;
    class K1,K2,K3,PG1,PG2,RS storage;
```

Kafka is part of the real processing path. A browser does not advance the
dataset and a FastAPI process does not run inference.

Delivery is at least once. Stable event IDs, consumer inboxes, database
uniqueness, conditional upserts, deterministic results, and a transactional
outbox make redelivery harmless. The implementation does not claim exactly-once
delivery.

See [architecture.md](docs/architecture.md), the
[migration plan](docs/migration-plan.md), and the [ADRs](docs/adr/) for the
current/target flow, failure windows, topic contracts, backpressure, scaling,
security, and tradeoffs.

## Reproducible local environment

Requirements:

- Docker Engine with Compose
- At least 6 GB available memory for the full stack and model worker

Start Kafka (KRaft), PostgreSQL, Redis, migrations, topic bootstrap, all
application services, the frontend, Prometheus, Grafana, OpenTelemetry
Collector, and Tempo:

```bash
docker compose up --build
```

The default image contains a 9,600-node deterministic Elliptic-shaped fixture:
100 ordered timesteps with 96 transactions and 461 real, same-timestep edges
per batch. At the default 20 transactions/second it provides about eight
minutes of progressive visual replay. Its scaler-distribution feature samples
make the unchanged model exercise clear, elevated, and high-risk states. It
exercises the real checkpoint, Kafka, database, model, Redis, API, and frontend
path; it is not presented as production data.

| Surface | URL |
|---|---|
| Analyst dashboard | http://localhost:3000 |
| FastAPI gateway | http://localhost:8000 |
| Prometheus | http://localhost:9090 |
| Grafana (`admin` / `admin`, local only) | http://localhost:3001 |
| Tempo API | http://localhost:3200 |

Verify that a canonical score reached PostgreSQL and Redis, REST can read it,
and a cursor-replay WebSocket publishes it:

```bash
docker compose run --rm -T test-runner \
  python -m scripts.verify_pipeline \
  --api-url http://gateway-lb:8080 \
  --websocket-url ws://gateway-lb:8080/api/v1/ws
```

Scale inference independently:

```bash
docker compose up -d --scale inference-worker=3
```

Remove the local containers and durable volumes:

```bash
docker compose down -v
```

### Full Elliptic dataset

Set `ELLIPTIC_DATA_DIR` to the directory containing
`elliptic_bitcoin_dataset/`, then include the read-only dataset override:

```bash
ELLIPTIC_DATA_DIR=/path/to/data \
docker compose \
  -f docker-compose.yml \
  -f infra/docker/compose.dataset.yml \
  up --build
```

The dataset directory must contain:

```text
elliptic_bitcoin_dataset/
  elliptic_txs_features.csv
  elliptic_txs_edgelist.csv
  elliptic_txs_classes.csv
```

Replay progress and speed are persisted. The configured rate is transactions
per second even though Kafka preserves timestep-level graph batches. A browser
never advances the source iterator. On a full page load the gateway provides a
bounded recent replay and the client presents it at the selected rate, restoring
the one-transaction-at-a-time investigation behavior without rerunning
inference. The local Redis and browser limits are 10,000 events/nodes, so the
entire 9,600-transaction bundled fixture remains available in the graph. Larger
external datasets remain deliberately bounded and require a production
level-of-detail or server-side graph-windowing policy rather than unbounded
browser memory.

## Developer checks

Python 3.11 is required for direct host development:

```bash
python3.11 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt

ruff format --check services shared scripts migrations tests
ruff check services shared scripts migrations tests
mypy services shared scripts --ignore-missing-imports
pytest tests/unit tests/contract tests/failure -q
```

Frontend:

```bash
cd frontend
npm ci
npm test -- --watchAll=false --passWithNoTests
npm run build
```

Distributed integration tests:

```bash
docker compose up --build -d --scale inference-worker=3
docker compose run --rm -T test-runner pytest tests/integration -q
```

Contract JSON Schemas are generated from Pydantic models:

```bash
python -m scripts.generate_schemas
```

Do not regenerate `shared/contracts/compatibility-baseline/` for a v1 change;
that immutable snapshot is the breaking-change guard.

## Failure and performance verification

`python -m scripts.failure_probe` issues an accepted Kafka request that can be checked
after inference workers are killed and restarted. The distributed GitHub
Actions workflow automates this window and verifies the resulting canonical
score.

The benchmark reports real measurements or `null`, never target-shaped
estimates:

```bash
python -m scripts.benchmark \
  --worker-count 3 \
  --require-target \
  --output benchmark.json
```

It separates PostgreSQL one-/two-hop query latency, Kafka queue delay, model
inference, result persistence, Redis publication, ingest-to-Redis, and live
Redis-to-WebSocket latency. The sub-270 ms claim is supported only by a run
whose recorded p95 meets the target; weighted F1 remains an offline model
quality metric, not an infrastructure latency metric.

The latest local verification, including the failure windows and measured
latency/scaling results, is recorded in
[verification.md](docs/verification.md).

## API

Backward-compatible paths remain available alongside `/api/v1`.

| Endpoint | Behavior |
|---|---|
| `GET /health/live` | Process liveness; independent of temporary dependencies |
| `GET /health/ready` | PostgreSQL and Redis serving readiness |
| `GET /health` | Backward-compatible aggregate health |
| `GET /entity/{tx_id}` | Durable transaction, score, and direct neighbors |
| `GET /subgraph/{tx_id}` | Bounded deterministic graph with `truncated` |
| `WS /ws` | Backward-compatible scored-event stream |
| `WS /api/v1/ws?last_event_id=...` | Live stream with bounded cursor replay |
| `GET /metrics` | Gateway Prometheus metrics |

WebSocket controls are strictly validated:

```json
{"type":"set_threshold","value":0.85}
{"type":"set_speed","interval":0.1}
{"type":"pause_replay"}
{"type":"resume_replay"}
```

Threshold changes are presentation-only and never rerun inference.

## Frontend demo mode

The isolated visual fixture remains opt-in when backend infrastructure is not
available:

```bash
cd frontend
REACT_APP_DEMO_MODE=true npm start
```

Use `REACT_APP_DEMO_STATE=disconnected` to inspect the explicit disconnected
state. Production behavior is the default.

## Model

The migration does not retrain or alter the model, scaler, preprocessing, or
checkpoint behavior.

- Architecture: three-layer GAT-ResNet with residual and input-skip connections
- Dataset: Elliptic Bitcoin Dataset, 49 temporal steps
- Split: train 1–34, validation 35–41, test 42–49
- Feature schema: `elliptic-165-v1`
- Stored versioning: model version, SHA-256 checksum, feature schema, graph
  watermark, and deployment-selected presentation threshold

Committed offline evaluation at threshold 0.90:

| Metric | Value |
|---|---:|
| AUC-PR (illicit) | 0.874 |
| MCC | 0.609 |
| Illicit precision | 68% |
| Illicit recall | 60% |
| Illicit F1 | 0.639 |
| Weighted F1 | 0.942 |
