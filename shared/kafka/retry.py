from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from typing import TypeVar

T = TypeVar("T")


class PermanentEventError(Exception):
    """The event cannot succeed without changing its data or code."""


class MissingGraphStateError(PermanentEventError):
    """The requested transaction or required graph state is absent."""


class ModelInferenceError(PermanentEventError):
    """The model rejected a structurally valid, durable scoring request."""


def retry_delay_seconds(attempt: int, base_ms: int, maximum_ms: int) -> float:
    exponential = min(maximum_ms, base_ms * (2 ** max(attempt - 1, 0)))
    jitter = random.uniform(0.75, 1.25)
    return min(exponential * jitter, maximum_ms) / 1_000


async def with_bounded_retry(
    operation: Callable[[], Awaitable[T]],
    *,
    attempts: int,
    base_delay_ms: int,
    max_delay_ms: int,
) -> T:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return await operation()
        except PermanentEventError:
            raise
        except Exception as exc:
            last_error = exc
            if attempt == attempts:
                break
            await asyncio.sleep(retry_delay_seconds(attempt, base_delay_ms, max_delay_ms))
    assert last_error is not None
    raise last_error
