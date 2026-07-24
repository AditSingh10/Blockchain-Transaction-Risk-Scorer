# ADR 0006: Redis Streams bridge results to WebSockets

Status: Accepted

Every gateway replica tails the same bounded Redis Stream for its local clients.
Stream IDs give reconnect cursors and bounded missed-event replay, while
PostgreSQL remains canonical if Redis history is trimmed or lost. Pub/Sub alone
would lose reconnect history; using Kafka directly from each browser gateway
would complicate broadcast semantics and consumer offsets.
