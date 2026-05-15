"""Per-lead dispatch flow — concurrency, device select, history, push.

Spec: retry-followup § Requirement "scheduler 调度数据流";
      service-communication § Requirement "全局并发控制";
      service-communication § Scenario "scheduler 不再 HTTP 调 telephony-api"
      (arch-cloud-edge-split: device select via cloud PG, not telephony-api HTTP);
      time-window § Requirement "窗外 lead 推迟到下个窗口开始时刻";
      design.md Decision 8 (失败即 DECR 回滚) / Decision 12 (派发后写
      lead.status='calling', MUST NOT 写终态).
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from isales_common.enums import LeadStatus
from isales_common.models.campaign import Campaign
from isales_common.models.lead import Lead
from isales_common.schemas.messages.dial import DialLeadInfo, DialRequest
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from isales_scheduler import concurrency, time_window
from isales_scheduler.device_selection import pick_idle_device
from isales_scheduler.history import pack_history
from isales_scheduler.prompt import pack_prompt_versions

logger = logging.getLogger(__name__)

DIAL_QUEUE = "engine:dial"


async def dispatch_lead(
    *,
    session: AsyncSession,
    redis: Redis[Any],
    campaign: Campaign,
    lead: Lead,
    now: datetime,
    holiday_dates: set[Any],
    max_concurrency: int,
    history_n: int,
) -> bool:
    """Try to dispatch a single lead. Returns True on success, False otherwise.

    Failure paths NEVER touch lead.status (kept at original new/retrying/
    following_up so the next tick retries naturally). Only success path
    writes ``status='calling'``. Out-of-window leads get their next_call_at
    bumped to the next window start (the only ``next_call_at`` write
    scheduler is allowed per retry-followup spec).
    """

    # 1) time-window judgment
    in_window = time_window.is_in_window(campaign.time_windows, now)
    if not in_window:
        new_next = time_window.next_window_start(
            campaign.time_windows,
            now,
            respect_holidays=campaign.respect_holidays,
            holiday_dates=holiday_dates,
        )
        if new_next is not None:
            lead.next_call_at = new_next
            await session.flush()
            logger.info(
                "lead_deferred_out_of_window lead_id=%d new_next=%s",
                lead.id,
                new_next.isoformat(),
            )
        return False

    # 2) acquire concurrency slot
    acquired = await concurrency.try_increment(redis, max_concurrency)
    if not acquired:
        logger.info("dispatch_skip_concurrency_full lead_id=%d", lead.id)
        return False

    try:
        # 3) device select — cloud PG direct query (replaces HTTP /devices/select)
        device_resp = await pick_idle_device(session, campaign.id)
        if device_resp is None:
            await concurrency.decrement(redis)
            logger.warning(
                "dispatch_skip_device_select lead_id=%d campaign_id=%d",
                lead.id,
                campaign.id,
            )
            return False

        # 4) pack history + prompt snapshot
        history = await pack_history(session, lead, limit=history_n)
        prompt_versions = await pack_prompt_versions(session, campaign.id)

        # 5) build DialRequest
        msg = DialRequest(
            lead=DialLeadInfo(
                lead_id=lead.id,
                campaign_id=campaign.id,
                phone=lead.phone,
                name=lead.name,
                custom_data=lead.custom_data or {},
            ),
            history=history,
            prompt_versions=prompt_versions,
            caller_id=device_resp.phone_number,
            device_id=device_resp.device_id,
        )

        # 6) push to engine:dial queue
        await redis.lpush(DIAL_QUEUE, msg.model_dump_json())

        # 7) flip lead.status → calling (only allowed transition for scheduler)
        lead.status = LeadStatus.CALLING
        await session.flush()

        logger.info(
            "dispatched lead_id=%d campaign_id=%d device_id=%d caller_id=%s message_id=%s",
            lead.id,
            campaign.id,
            device_resp.device_id,
            device_resp.phone_number,
            msg.message_id,
        )
        return True
    except Exception:
        await concurrency.decrement(redis)
        logger.exception("dispatch_error lead_id=%d", lead.id)
        return False
