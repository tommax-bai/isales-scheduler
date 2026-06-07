"""CampaignControl consumption — keeps active campaign set in sync.

Active set lives in Redis SET ``scheduler:active-campaigns`` (persistent across
restarts) plus an in-process cache for fast tick reads. Each CampaignControl
message updates both.

Spec: service-communication § Requirement "通信通道矩阵" (api → scheduler queue);
      message-contract § Scenario "consumer 校验 schema_version" (DLQ on
      unsupported version);
      design.md Decision 2 (Redis SET as source of truth — Campaign model
      has no status field in v0.1.2).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from isales_common.redis_keys import SCHEDULER_ACTIVE_CAMPAIGNS_SET
from isales_common.schemas.messages import (
    CampaignControl,
    PauseCampaign,
    ResumeCampaign,
    StartCampaign,
)
from pydantic import TypeAdapter, ValidationError
from redis.asyncio import Redis

logger = logging.getLogger(__name__)

CONTROL_QUEUE = "scheduler:campaign-control"
DLQ = "scheduler:dlq"
# Shared with isales-api (read-only there); see isales_common.redis_keys.
ACTIVE_SET = SCHEDULER_ACTIVE_CAMPAIGNS_SET
SUPPORTED_SCHEMA_VERSIONS = {1}

_CONTROL_ADAPTER: TypeAdapter[CampaignControl] = TypeAdapter(CampaignControl)


class ActiveCampaigns:
    """Redis-backed active campaign set with in-process cache."""

    def __init__(self, redis: Redis[Any]) -> None:
        self._redis = redis
        self._cache: set[int] = set()
        self._lock = asyncio.Lock()

    async def restore_from_redis(self) -> None:
        members = await self._redis.smembers(ACTIVE_SET)
        async with self._lock:
            self._cache = {int(m) for m in members}
        logger.info("active_campaigns_restored count=%d", len(self._cache))

    async def add(self, campaign_id: int) -> None:
        await self._redis.sadd(ACTIVE_SET, campaign_id)
        async with self._lock:
            self._cache.add(campaign_id)

    async def remove(self, campaign_id: int) -> None:
        await self._redis.srem(ACTIVE_SET, campaign_id)
        async with self._lock:
            self._cache.discard(campaign_id)

    async def snapshot(self) -> set[int]:
        async with self._lock:
            return set(self._cache)


def _parse_schema_version(raw: str) -> int | None:
    """Read just the schema_version field without full validation.

    Used to gate DLQ before pydantic discriminator chokes on unknown shape.
    """

    import json as _json

    try:
        parsed = _json.loads(raw)
    except (ValueError, TypeError):
        return None
    sv = parsed.get("schema_version") if isinstance(parsed, dict) else None
    return sv if isinstance(sv, int) else None


async def _handle_message(
    raw: str,
    active: ActiveCampaigns,
    redis: Redis[Any],
    wake_event: asyncio.Event,
) -> None:
    sv = _parse_schema_version(raw)
    if sv is None or sv not in SUPPORTED_SCHEMA_VERSIONS:
        logger.warning("control_dlq schema_version=%s raw_len=%d", sv, len(raw))
        await redis.lpush(DLQ, raw)
        return

    try:
        msg = _CONTROL_ADAPTER.validate_json(raw)
    except ValidationError as exc:
        logger.warning("control_dlq validation_error errors=%s", exc.errors()[:3])
        await redis.lpush(DLQ, raw)
        return

    if isinstance(msg, (StartCampaign, ResumeCampaign)):
        await active.add(msg.campaign_id)
        logger.info("active_added campaign_id=%d type=%s", msg.campaign_id, msg.type)
        wake_event.set()  # 立即唤醒 scheduler tick
    elif isinstance(msg, PauseCampaign):
        await active.remove(msg.campaign_id)
        logger.info("active_removed campaign_id=%d", msg.campaign_id)


async def control_loop(active: ActiveCampaigns, redis: Redis[Any], wake_event: asyncio.Event) -> None:
    """BLPOP loop. Cancellable; surfaces non-fatal errors to logger.

    BLPOP timeout is short (1s) so asyncio cancellation propagates promptly
    on shutdown.
    """

    while True:
        try:
            popped = await redis.blpop([CONTROL_QUEUE], timeout=1)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("control_blpop_error")
            await asyncio.sleep(1)
            continue

        if popped is None:
            continue
        _key, raw = popped
        try:
            await _handle_message(raw, active, redis, wake_event)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("control_handle_error raw_len=%d", len(raw))
