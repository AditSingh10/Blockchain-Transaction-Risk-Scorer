# ADR 0003: At-least-once delivery with idempotent effects

Status: Accepted

Consumers commit offsets only after their durable downstream action. Crashes in
the commit window therefore cause redelivery. Stable IDs, inbox keys, unique
constraints, conditional upserts, and deterministic result IDs make that
redelivery safe. The system does not claim end-to-end exactly once because
Kafka, PostgreSQL, and Redis do not share one distributed transaction.
