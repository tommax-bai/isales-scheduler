"""Main scheduler tick loop.

For each active campaign: skip on holiday, select due leads, dispatch in
batch order. Failure on a single lead breaks the rest of that campaign's
batch this tick (keeps the tick from spinning on a flaky dependency).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, date, datetime, timedelta
from typing import Any

from isales_common.enums import LeadStatus
from isales_common.models.campaign import Campaign
from isales_common.models.holiday import Holiday
from isales_common.models.lead import Lead
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from isales_scheduler import time_window
from isales_scheduler.control import ActiveCampaigns
from isales_scheduler.dispatch import dispatch_lead
from isales_scheduler.settings import Settings

logger = logging.getLogger(__name__)


async def _load_holiday_dates(
    sessionmaker: async_sessionmaker[Any],
    *,
    today: date,
    horizon_days: int = 60,
) -> set[date]:
    upper = today + timedelta(days=horizon_days)
    async with sessionmaker() as session:
        stmt = (
            select(Holiday.date)
            .where(Holiday.date >= today)
            .where(Holiday.date <= upper)
        )
        rows = (await session.execute(stmt)).scalars().all()
    return set(rows)


async def tick(
    *,
    sessionmaker: async_sessionmaker[Any],
    redis: Redis[Any],
    active: ActiveCampaigns,
    settings: Settings,
    now: datetime | None = None,
) -> None:
    """One scheduling pass over all active campaigns."""

    now = now or datetime.now(UTC).astimezone()
    today = now.date()

    active_ids = await active.snapshot()
    if not active_ids:
        return

    holiday_dates = await _load_holiday_dates(sessionmaker, today=today)

    for campaign_id in sorted(active_ids):
        async with sessionmaker() as session:
            campaign = await session.get(Campaign, campaign_id)
            if campaign is None:
                logger.warning("active_campaign_missing campaign_id=%d", campaign_id)
                continue

            if time_window.is_holiday_for_campaign(
                campaign.respect_holidays, today, holiday_dates
            ):
                logger.info("holiday_skip campaign_id=%d date=%s", campaign_id, today)
                continue

            stmt = (
                select(Lead)
                .where(Lead.campaign_id == campaign_id)
                .where(
                    Lead.status.in_(
                        [LeadStatus.NEW, LeadStatus.RETRYING, LeadStatus.FOLLOWING_UP]
                    )
                )
                .where(Lead.next_call_at.is_not(None))
                .where(Lead.next_call_at <= now)
                .order_by(Lead.next_call_at.asc())
                .limit(settings.scheduler_batch_size)
            )
            leads = list((await session.execute(stmt)).scalars().all())
            if not leads:
                continue

            for lead in leads:
                ok = await dispatch_lead(
                    session=session,
                    redis=redis,
                    campaign=campaign,
                    lead=lead,
                    now=now,
                    holiday_dates=holiday_dates,
                    max_concurrency=settings.max_concurrency,
                    history_n=settings.scheduler_history_n,
                )
                if not ok:
                    break

            await session.commit()


async def scheduler_loop(
    *,
    sessionmaker: async_sessionmaker[Any],
    redis: Redis[Any],
    active: ActiveCampaigns,
    settings: Settings,
) -> None:
    while True:
        try:
            await tick(
                sessionmaker=sessionmaker,
                redis=redis,
                active=active,
                settings=settings,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("tick_failed")
        await asyncio.sleep(settings.scheduler_tick_interval)
