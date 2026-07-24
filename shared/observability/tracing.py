from __future__ import annotations

from collections.abc import Mapping

from opentelemetry import propagate, trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from shared.config.settings import Settings


def configure_tracing(settings: Settings) -> None:
    if not settings.otel_exporter_endpoint:
        return
    provider = TracerProvider(
        resource=Resource.create(
            {
                "service.name": settings.service_name,
                "deployment.environment": settings.environment,
            }
        )
    )
    provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=settings.otel_exporter_endpoint))
    )
    trace.set_tracer_provider(provider)


def inject_kafka_trace(headers: list[tuple[str, bytes]]) -> list[tuple[str, bytes]]:
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return [*headers, *((key, value.encode()) for key, value in carrier.items())]


def current_traceparent() -> str | None:
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return carrier.get("traceparent")


def extract_kafka_context(headers: list[tuple[str, bytes]] | None):
    carrier: Mapping[str, str] = {
        key: value.decode(errors="replace") for key, value in (headers or [])
    }
    return propagate.extract(carrier)


def extract_traceparent(traceparent: str | None):
    return propagate.extract({"traceparent": traceparent} if traceparent else {})
