"""Integration test for one tick — multi-campaign + holiday + concurrency."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import pytest
from isales_common.enums import LeadStatus
from isales_common.models.campaign import Campaign
from isales_common.models.holiday import Holiday
from isales_common.models.lead import Lead

from isales_scheduler import concurrency
from isales_scheduler.control import ActiveCampaigns
from isales_scheduler.dispatch import DIAL_QUEUE
from isales_scheduler.loop import tick
from isales_scheduler.settings import Settings
from isales_scheduler.telephony import TelephonyClient

TZ = ZoneInfo("Asia/Shanghai")

ALWAYS_OPEN: list[dict[str, Any]] = [
    {"days": ["mon", "tue", "wed", "thu", "fri", "sat", "sun"],
     "start": "00:00", "end": "23:59"},
]


def _settings(max_conc: int = 8, batch: int = 50, hist: int = 3) -> Settings:
    import os
    os.environ.setdefault("ISALES_DATABASE_URL", "postgresql+asyncpg://test/test")
    os.environ.setdefault("ISALES_REDIS_URL", "redis://localhost:6379/0")
    s = Settings()
    s.max_concurrency = max_conc
    s.scheduler_batch_size = batch
    s.scheduler_history_n = hist
    return s


def _client_for(devices: list[dict[str, int | str]]):  # type: ignore[no-untyped-def]
    """A telephony mock that returns devices[0], devices[1], … in order."""

    iterator = iter(devices)

    def handler(_req: httpx.Request) -> httpx.Response:
        try:
            d = next(iterator)
        except StopIteration:
            return httpx.Response(503)
        return httpx.Response(200, json=d)

    transport = httpx.MockTransport(handler)
    c = TelephonyClient("http://t.test", timeout=1.0)
    c._client = httpx.AsyncClient(transport=transport, timeout=1.0)
    return c


@pytest.mark.asyncio(loop_scope="session")
async def test_tick_dispatches_window_skips_holiday_and_caps_concurrency(  # type: ignore[no-untyped-def]
    sessionmaker_, redis_client
) -> None:
    now = datetime(2026, 5, 4, 10, 0, tzinfo=TZ)  # Monday

    async with sessionmaker_() as session:
        # Two active campaigns. Camp B respects holidays and today is a holiday.
        camp_a = Campaign(name="A", time_windows=ALWAYS_OPEN, respect_holidays=False)
        camp_b = Campaign(name="B", time_windows=ALWAYS_OPEN, respect_holidays=True)
        session.add_all([camp_a, camp_b])
        await session.flush()

        # Camp A: 5 leads due
        for i in range(5):
            session.add(
                Lead(
                    campaign_id=camp_a.id,
                    phone=f"138000000{i:02d}",
                    status=LeadStatus.NEW,
                    next_call_at=now,
                )
            )
        # Camp B: 3 leads due (should be skipped because of holiday)
        for i in range(3):
            session.add(
                Lead(
                    campaign_id=camp_b.id,
                    phone=f"137000000{i:02d}",
                    status=LeadStatus.NEW,
                    next_call_at=now,
                )
            )
        # Holiday on now's date → blocks camp_b
        session.add(Holiday(date=date(2026, 5, 4), name="Test Holiday", region="CN"))
        await session.commit()
        a_id, b_id = camp_a.id, camp_b.id

    # Activate both campaigns
    active = ActiveCampaigns(redis_client)
    await active.add(a_id)
    await active.add(b_id)

    # Concurrency cap = 3 → only 3 leads of camp_a should dispatch this tick
    devices = [{"device_id": i + 1, "phone_number": f"139000000{i:02d}"} for i in range(10)]
    telephony = _client_for(devices)
    settings = _settings(max_conc=3)

    await tick(
        sessionmaker=sessionmaker_,
        redis=redis_client,
        telephony=telephony,
        active=active,
        settings=settings,
        now=now,
    )
    await telephony.aclose()

    # Counter saturated at cap
    assert await concurrency.get(redis_client) == 3
    # Exactly 3 DialRequest pushed
    assert await redis_client.llen(DIAL_QUEUE) == 3

    # Camp A: 3 leads moved to calling, 2 stayed new
    async with sessionmaker_() as session:
        from sqlalchemy import select
        a_calling = await session.execute(
            select(Lead).where(Lead.campaign_id == a_id, Lead.status == LeadStatus.CALLING)
        )
        a_new = await session.execute(
            select(Lead).where(Lead.campaign_id == a_id, Lead.status == LeadStatus.NEW)
        )
        b_all = await session.execute(
            select(Lead).where(Lead.campaign_id == b_id)
        )
    assert len(a_calling.scalars().all()) == 3
    assert len(a_new.scalars().all()) == 2
    # Camp B fully untouched (holiday skipped)
    b_leads = b_all.scalars().all()
    assert len(b_leads) == 3
    for lead in b_leads:
        assert lead.status == LeadStatus.NEW
