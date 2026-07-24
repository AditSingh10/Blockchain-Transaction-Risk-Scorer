from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

SCHEMA_VERSION_V1: Literal["1.0"] = "1.0"


def utc_now() -> datetime:
    return datetime.now(UTC)


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class EventEnvelopeV1(StrictModel):
    event_id: str = Field(min_length=36, max_length=36)
    event_type: str = Field(min_length=3, max_length=96)
    schema_version: Literal["1.0"] = SCHEMA_VERSION_V1
    occurred_at: datetime
    produced_at: datetime = Field(default_factory=utc_now)
    producer: str = Field(min_length=2, max_length=64)
    trace_id: str = Field(min_length=16, max_length=64)
    traceparent: str | None = Field(
        default=None,
        pattern=r"^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$",
    )
    correlation_id: str = Field(min_length=1, max_length=128)
    causation_id: str | None = Field(default=None, max_length=128)

    @field_validator("occurred_at", "produced_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("event timestamps must be timezone-aware")
        return value

    def kafka_headers(self) -> list[tuple[str, bytes]]:
        headers = [
            ("event_type", self.event_type.encode()),
            ("schema_version", self.schema_version.encode()),
            ("event_id", self.event_id.encode()),
            ("trace_id", self.trace_id.encode()),
            ("correlation_id", self.correlation_id.encode()),
        ]
        if self.traceparent:
            headers.append(("traceparent", self.traceparent.encode()))
        return headers


class GraphNodeV1(StrictModel):
    tx_id: str = Field(min_length=1, max_length=64)
    time_step: int = Field(ge=1)
    features: list[float] = Field(min_length=165, max_length=165)


class GraphEdgeV1(StrictModel):
    source_tx_id: str = Field(min_length=1, max_length=64)
    destination_tx_id: str = Field(min_length=1, max_length=64)


class GraphBatchPayloadV1(StrictModel):
    stream_name: str = Field(min_length=1, max_length=64)
    time_step: int = Field(ge=1)
    sequence: int = Field(ge=1)
    source_offset: int = Field(ge=0)
    feature_schema_version: str = Field(min_length=1, max_length=64)
    nodes: list[GraphNodeV1] = Field(min_length=1, max_length=100_000)
    edges: list[GraphEdgeV1] = Field(default_factory=list, max_length=250_000)


class GraphBatchObservedV1(EventEnvelopeV1):
    event_type: Literal["risk.graph-batch.observed.v1"] = "risk.graph-batch.observed.v1"
    payload: GraphBatchPayloadV1


class ScoringRequestedPayloadV1(StrictModel):
    tx_id: str = Field(min_length=1, max_length=64)
    graph_watermark: int = Field(ge=1)
    source_time_step: int = Field(ge=1)
    event_sequence: int = Field(ge=1)
    feature_schema_version: str = Field(min_length=1, max_length=64)
    source_ingested_at: datetime
    requested_at: datetime


class ScoringRequestedV1(EventEnvelopeV1):
    event_type: Literal["risk.scoring.requested.v1"] = "risk.scoring.requested.v1"
    payload: ScoringRequestedPayloadV1


class ScoringCompletedPayloadV1(StrictModel):
    tx_id: str = Field(min_length=1, max_length=64)
    illicit_probability: float = Field(ge=0.0, le=1.0)
    model_version: str = Field(min_length=1, max_length=128)
    model_checksum: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    model_deployed_at: datetime | None = None
    feature_schema_version: str = Field(min_length=1, max_length=64)
    graph_watermark: int = Field(ge=1)
    source_time_step: int = Field(ge=1)
    event_sequence: int = Field(ge=1)
    source_ingested_at: datetime
    queue_delay_ms: float = Field(ge=0)
    inference_latency_ms: float = Field(ge=0)
    end_to_end_latency_ms: float = Field(ge=0)
    scored_at: datetime


class ScoringCompletedV1(EventEnvelopeV1):
    event_type: Literal["risk.scoring.completed.v1"] = "risk.scoring.completed.v1"
    payload: ScoringCompletedPayloadV1


class ScoringFailedPayloadV1(StrictModel):
    tx_id: str = Field(min_length=1, max_length=64)
    failure_stage: str = Field(min_length=1, max_length=64)
    error_class: str = Field(min_length=1, max_length=128)
    sanitized_error: str = Field(min_length=1, max_length=512)
    attempt_count: int = Field(ge=1)
    retryable: bool
    failed_at: datetime


class ScoringFailedV1(EventEnvelopeV1):
    event_type: Literal["risk.scoring.failed.v1"] = "risk.scoring.failed.v1"
    payload: ScoringFailedPayloadV1


class DeadLetterPayloadV1(StrictModel):
    original_topic: str = Field(min_length=1, max_length=249)
    original_partition: int = Field(ge=0)
    original_offset: int = Field(ge=0)
    original_key: str | None = Field(default=None, max_length=512)
    original_payload: dict[str, Any] | str
    original_event_id: str | None = Field(default=None, max_length=128)
    failure_stage: str = Field(min_length=1, max_length=64)
    exception_class: str = Field(min_length=1, max_length=128)
    sanitized_error: str = Field(min_length=1, max_length=512)
    attempt_count: int = Field(ge=1)
    first_failure_at: datetime
    last_failure_at: datetime
    replayed_by: str | None = Field(default=None, max_length=128)


class DeadLetterEventV1(EventEnvelopeV1):
    event_type: Literal["risk.dead-lettered.v1"] = "risk.dead-lettered.v1"
    payload: DeadLetterPayloadV1


EVENT_MODELS = (
    GraphBatchObservedV1,
    ScoringRequestedV1,
    ScoringCompletedV1,
    ScoringFailedV1,
    DeadLetterEventV1,
)


def decode_event(raw: bytes, model: type[StrictModel]) -> StrictModel:
    return model.model_validate_json(raw)
