# ADR 0001: Kafka is the durable event backbone

Status: Accepted

Kafka sits on the real graph/scoring/result path, buffers accepted work across
worker failures, provides consumer-group partition ownership, and exposes lag
as saturation. Direct in-process queues cannot survive restart or support
independent inference scaling. The cost is an additional operational dependency
and at-least-once duplicate handling.
