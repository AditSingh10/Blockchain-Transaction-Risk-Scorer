# Kubernetes application examples

These manifests deploy only application workloads. Kafka, PostgreSQL, Redis,
and the OpenTelemetry backend are intentionally referenced as managed,
production-grade dependencies; the single-node Compose instances are local
development infrastructure and are not represented as highly available.

Replace the example image and endpoints, create `risk-monitor-secrets` through
your secret manager, run migrations and topic bootstrap as controlled release
jobs, then apply the base manifests.

CPU is a portable initial autoscaling signal. Kafka consumer lag is the
preferred production signal for inference workers (for example through KEDA or
a Prometheus adapter) because lag directly measures queued durable work.
