from __future__ import annotations

from shared.contracts.ids import deterministic_event_id


def test_duplicate_outbox_publication_has_same_downstream_event_id() -> None:
    request_id = deterministic_event_id("request", "graph-event", "tx-1", 7)
    first_result = deterministic_event_id("result", request_id, "model-v1")
    retried_result = deterministic_event_id("result", request_id, "model-v1")
    assert first_result == retried_result
