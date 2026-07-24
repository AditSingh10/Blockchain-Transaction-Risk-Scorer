from __future__ import annotations

import pytest
from pydantic import ValidationError

from shared.config.settings import Settings
from shared.contracts.ids import deterministic_event_id
from shared.database.queries import canonical_edge
from shared.kafka.dlq import sanitize_error
from shared.kafka.retry import (
    PermanentEventError,
    retry_delay_seconds,
    with_bounded_retry,
)


def test_deterministic_event_ids_are_stable_and_namespaced() -> None:
    first = deterministic_event_id("graph", "stream", 7)
    assert first == deterministic_event_id("graph", "stream", 7)
    assert first != deterministic_event_id("score", "stream", 7)


def test_directed_edge_is_not_reordered() -> None:
    assert canonical_edge("z-source", "a-destination") == (
        "z-source",
        "a-destination",
    )
    with pytest.raises(ValueError):
        canonical_edge("same", "same")


def test_retry_delay_is_bounded() -> None:
    for attempt in range(1, 20):
        assert 0 <= retry_delay_seconds(attempt, 100, 5_000) <= 5


def test_configuration_rejects_unordered_graph_topic() -> None:
    with pytest.raises(ValidationError):
        Settings(graph_batch_partitions=2)


def test_configuration_rejects_unbounded_values() -> None:
    with pytest.raises(ValidationError):
        Settings(max_in_flight_inference=0)


def test_sasl_configuration_fails_fast_when_credentials_are_missing() -> None:
    with pytest.raises(ValidationError, match="kafka_sasl_password"):
        Settings(
            kafka_security_protocol="SASL_SSL",
            kafka_sasl_mechanism="SCRAM-SHA-512",
            kafka_sasl_username="worker",
        )


def test_logged_errors_redact_credentials() -> None:
    error = RuntimeError("postgresql://user:password@db/risk?token=secret-value")
    sanitized = sanitize_error(error)
    assert "user:password" not in sanitized
    assert "secret-value" not in sanitized


@pytest.mark.asyncio
async def test_permanent_failure_is_not_retried() -> None:
    attempts = 0

    async def operation() -> None:
        nonlocal attempts
        attempts += 1
        raise PermanentEventError("poison event")

    with pytest.raises(PermanentEventError):
        await with_bounded_retry(
            operation,
            attempts=4,
            base_delay_ms=10,
            max_delay_ms=100,
        )
    assert attempts == 1


@pytest.mark.asyncio
async def test_transient_failure_uses_bounded_attempt_count() -> None:
    attempts = 0

    async def operation() -> None:
        nonlocal attempts
        attempts += 1
        raise ConnectionError("temporary")

    with pytest.raises(ConnectionError):
        await with_bounded_retry(
            operation,
            attempts=3,
            base_delay_ms=10,
            max_delay_ms=10,
        )
    assert attempts == 3
