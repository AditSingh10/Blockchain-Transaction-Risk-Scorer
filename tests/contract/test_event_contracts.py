from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from shared.contracts.compatibility import backward_compatibility_errors
from shared.contracts.events import (
    EVENT_MODELS,
    GraphBatchObservedV1,
    GraphBatchPayloadV1,
    GraphNodeV1,
)
from shared.contracts.ids import deterministic_event_id

SCHEMA_ROOT = Path("shared/contracts/schemas")
BASELINE_ROOT = Path("shared/contracts/compatibility-baseline")


def graph_event() -> GraphBatchObservedV1:
    event_id = deterministic_event_id("graph", "fixture", 1)
    return GraphBatchObservedV1(
        event_id=event_id,
        occurred_at=datetime.now(UTC),
        producer="contract-test",
        trace_id=event_id.replace("-", ""),
        correlation_id="fixture:1",
        payload=GraphBatchPayloadV1(
            stream_name="fixture",
            time_step=1,
            sequence=1,
            source_offset=0,
            feature_schema_version="elliptic-165-v1",
            nodes=[GraphNodeV1(tx_id="1001", time_step=1, features=[0.0] * 165)],
        ),
    )


def test_event_round_trip_is_strict() -> None:
    event = graph_event()
    assert GraphBatchObservedV1.model_validate_json(event.model_dump_json()) == event
    invalid = event.model_dump()
    invalid["unexpected"] = True
    with pytest.raises(ValidationError):
        GraphBatchObservedV1.model_validate(invalid)


def test_feature_schema_shape_is_enforced() -> None:
    with pytest.raises(ValidationError):
        GraphNodeV1(tx_id="1001", time_step=1, features=[0.0] * 164)


@pytest.mark.parametrize("model", EVENT_MODELS)
def test_generated_schema_matches_models_and_v1_baseline(model) -> None:
    event_type = str(model.model_fields["event_type"].default)
    filename = f"{event_type}.schema.json"
    committed = json.loads((SCHEMA_ROOT / filename).read_text())
    baseline = json.loads((BASELINE_ROOT / filename).read_text())
    generated = model.model_json_schema()
    assert committed == generated
    assert backward_compatibility_errors(baseline, generated) == []


def test_compatibility_guard_detects_removal_and_type_change() -> None:
    baseline = {
        "properties": {
            "schema_version": {"const": "1.0", "type": "string"},
            "event_id": {"type": "string"},
        },
        "required": ["schema_version", "event_id"],
    }
    candidate = {
        "properties": {
            "schema_version": {"const": "1.0", "type": "string"},
            "event_id": {"type": "integer"},
        },
        "required": ["schema_version"],
    }
    errors = backward_compatibility_errors(baseline, candidate)
    assert "$.event_id is no longer required" in errors
    assert "event_id changed type" in errors
