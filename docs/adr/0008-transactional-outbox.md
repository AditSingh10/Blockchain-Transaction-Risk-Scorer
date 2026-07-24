# ADR 0008: Graph commits create a transactional outbox

Status: Accepted

Writing graph state and publishing a scoring request cannot be atomic across
PostgreSQL and Kafka. The materializer writes stable scoring intents in the
same database transaction as nodes, edges, watermark, and inbox. Relays publish
locked rows and mark them only after Kafka acknowledgement. Publish-before-mark
crashes can duplicate an event, so downstream idempotency remains mandatory.
