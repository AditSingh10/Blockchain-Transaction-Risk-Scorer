# ADR 0004: Graph materialization remains ordered

Status: Accepted

Elliptic time steps build cumulative graph context and scoring requests must not
precede their committed graph watermark. The graph-batch topic therefore starts
with one partition and one active consumer owner. Multiple materializer
replicas provide failover, not mutation parallelism. Inference—not graph
mutation—is the intended horizontal scale boundary.
