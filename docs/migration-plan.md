# Distributed pipeline migration plan

## Audit baseline

The original runtime had one FastAPI process owning the Elliptic CSV iterator,
`GraphBuffer`, `GNNInference`, a process-local score dictionary, and every
WebSocket. Connecting a browser advanced the dataset and ran inference inside
the WebSocket handler. A process restart erased graph and score state; API
replicas would see different state; no durable accepted-work boundary existed.

The trained checkpoint, scaler, preprocessing, offline metrics, graph
neighborhood semantics, REST paths, WebSocket path, and React workflows are
preserved.

## Migration sequence

1. Establish typed configuration, versioned contracts, PostgreSQL schema,
   inbox/outbox primitives, structured logging, metrics, and tracing.
2. Move replay to an independent Kafka producer and ordered graph writes to a
   materializer transaction.
3. Relay committed scoring intents from the outbox and run the unchanged model
   in a bounded, shared-consumer-group worker pool.
4. Project canonical results to PostgreSQL and a bounded Redis Stream.
5. Replace the stateful API process with PostgreSQL queries and cursor-aware
   Redis Stream WebSocket fan-out.
6. Package the vertical slice in Compose, add managed-dependency Kubernetes
   examples, contract/invariant/failure tests, CI, and a measurement harness.

Each phase keeps a single end-to-end path. The old `streaming/` modules remain
as historical code but are no longer imported by `api.main` or any production
service.
