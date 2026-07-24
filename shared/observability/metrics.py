from __future__ import annotations

from dataclasses import dataclass

from prometheus_client import REGISTRY, CollectorRegistry, Counter, Gauge, Histogram


@dataclass(frozen=True)
class ServiceMetrics:
    events_total: Counter
    failures_total: Counter
    duplicates_total: Counter
    retries_total: Counter
    duration_seconds: Histogram
    queue_delay_seconds: Histogram
    end_to_end_seconds: Histogram
    in_flight: Gauge
    backlog: Gauge


def build_service_metrics(
    service: str,
    registry: CollectorRegistry | None = None,
) -> ServiceMetrics:
    prefix = service.replace("-", "_")
    target_registry = registry or REGISTRY
    return ServiceMetrics(
        events_total=Counter(
            f"{prefix}_events_total",
            "Events completed by outcome",
            ["operation", "outcome"],
            registry=target_registry,
        ),
        failures_total=Counter(
            f"{prefix}_failures_total",
            "Failures classified by stage and exception",
            ["stage", "exception"],
            registry=target_registry,
        ),
        duplicates_total=Counter(
            f"{prefix}_duplicates_total",
            "Duplicate events skipped",
            ["operation"],
            registry=target_registry,
        ),
        retries_total=Counter(
            f"{prefix}_retries_total",
            "Bounded retry attempts",
            ["operation"],
            registry=target_registry,
        ),
        duration_seconds=Histogram(
            f"{prefix}_duration_seconds",
            "Operation duration",
            ["operation"],
            buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.27, 0.5, 1, 2, 5),
            registry=target_registry,
        ),
        queue_delay_seconds=Histogram(
            f"{prefix}_queue_delay_seconds",
            "Event queue delay",
            buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.27, 0.5, 1, 5, 30),
            registry=target_registry,
        ),
        end_to_end_seconds=Histogram(
            f"{prefix}_end_to_end_seconds",
            "End-to-end event latency",
            buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.27, 0.5, 1, 2, 5, 30),
            registry=target_registry,
        ),
        in_flight=Gauge(
            f"{prefix}_in_flight",
            "Current bounded in-flight work",
            registry=target_registry,
        ),
        backlog=Gauge(
            f"{prefix}_backlog",
            "Current durable backlog",
            ["queue"],
            registry=target_registry,
        ),
    )
