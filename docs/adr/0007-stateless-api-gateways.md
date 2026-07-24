# ADR 0007: FastAPI gateways do not load the model

Status: Accepted

Gateways query PostgreSQL, cache only short-lived subgraphs, tail Redis, and
hold only bounded connection-local state. They do not own replay, graph
mutation, canonical scores, or PyTorch. This permits horizontal replicas and
restart without sticky sessions or data loss, at the cost of dependency-aware
readiness and database query latency.
