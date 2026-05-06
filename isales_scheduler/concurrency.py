"""Global concurrency counter using Redis INCR/DECR.

Spec: service-communication § Requirement "全局并发控制";
      architecture (key name convention).

The key ``isales:concurrency:active`` is shared across services. Only this
module touches it from scheduler; engine will DECR after call hangup
(stage 4). Lower-bound protection on DECR uses an atomic Lua script — pure
GET-then-DECR would race under bursty load.
"""

from __future__ import annotations

import logging
from typing import Any

from redis.asyncio import Redis

logger = logging.getLogger(__name__)

CONCURRENCY_KEY = "isales:concurrency:active"

# Atomic increment-with-cap: INCR, if > cap then DECR back and return 0,
# else return new value. Returning 0 means "rejected" to the caller.
_INCR_IF_BELOW_LUA = """
local v = redis.call('INCR', KEYS[1])
if v > tonumber(ARGV[1]) then
  redis.call('DECR', KEYS[1])
  return 0
end
return v
"""

# Atomic floored decrement: DECR, but if result < 0 reset to 0.
_FLOORED_DECR_LUA = """
local v = redis.call('DECR', KEYS[1])
if v < 0 then
  redis.call('SET', KEYS[1], 0)
  return 0
end
return v
"""


async def try_increment(redis: Redis[Any], max_concurrency: int) -> bool:
    """Increment counter if it's below cap; otherwise leave it untouched.

    Returns True iff the slot was acquired.
    """

    result = await redis.eval(  # type: ignore[no-untyped-call]
        _INCR_IF_BELOW_LUA, 1, CONCURRENCY_KEY, max_concurrency
    )
    return int(result) > 0


async def decrement(redis: Redis[Any]) -> int:
    """Decrement counter with floor at 0. Returns post-decrement value."""

    result = await redis.eval(_FLOORED_DECR_LUA, 1, CONCURRENCY_KEY)  # type: ignore[no-untyped-call]
    return int(result)


async def get(redis: Redis[Any]) -> int:
    raw = await redis.get(CONCURRENCY_KEY)
    return int(raw) if raw else 0
