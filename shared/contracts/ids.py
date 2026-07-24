from __future__ import annotations

from uuid import UUID, uuid5

RISK_EVENT_NAMESPACE = UUID("eb73e48f-15f0-5c2e-aaf8-d71fe6052aa2")


def deterministic_event_id(event_type: str, *identity_parts: object) -> str:
    """Return a stable UUID for one logical event.

    Redelivery and producer retries therefore retain the same identifier while a
    deliberate DLQ replay can use a new identity and retain the original as its
    causation ID.
    """

    identity = ":".join([event_type, *(str(part) for part in identity_parts)])
    return str(uuid5(RISK_EVENT_NAMESPACE, identity))
