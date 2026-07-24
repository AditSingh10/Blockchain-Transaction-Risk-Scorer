from __future__ import annotations

import json
from typing import Any

from redis.asyncio import Redis

DEDUPLICATED_XADD = """
local existing = redis.call('GET', KEYS[2])
if existing then
  return existing
end
local stream_id = redis.call(
  'XADD', KEYS[1], 'MAXLEN', '~', ARGV[1], '*',
  'event', ARGV[2], 'event_id', ARGV[3]
)
redis.call('SET', KEYS[2], stream_id, 'EX', ARGV[4], 'NX')
return stream_id
"""


async def publish_result_to_stream(
    redis: Redis,
    *,
    stream_key: str,
    max_length: int,
    event_id: str,
    payload: dict[str, Any],
    dedup_ttl_seconds: int = 86_400,
) -> str:
    dedup_key = f"{stream_key}:published:{event_id}"
    stream_id = await redis.eval(
        DEDUPLICATED_XADD,
        2,
        stream_key,
        dedup_key,
        max_length,
        json.dumps(payload, separators=(",", ":")),
        event_id,
        dedup_ttl_seconds,
    )
    if isinstance(stream_id, bytes):
        return stream_id.decode()
    return str(stream_id)
