"""Integration test for one tick — multi-campaign + holiday + concurrency.

After arch-cloud-edge-split, device selection runs against cloud PG instead
of telephony-api HTTP, so the test seeds real device + sim_card +
campaign_device rows rather than mocking an HTTP transport.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

import pytest
from isales_common.enums import DeviceStatus, LeadStatus
from isales_common.models import (
    CampaignDevice,
    Device,
    DeviceSimBinding,
    SimCard,
)
from isales_common.models.campaign import Campaign
from isales_common.models.holiday import Holiday
from isales_common.models.lead import Lead

from isales_scheduler import concurrency
from isales_scheduler.control import ActiveCampaigns
from isales_scheduler.dispatch import DIAL_QUEUE
from isales_scheduler.loop import tick
from isales_scheduler.settings import Settings

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


async def _seed_devices(session, *, campaign_id: int, count: int) -> None:  # type: ignore[no-untyped-def]
    """Seed ``count`` idle devices bound to ``campaign_id`` with active SIMs."""
    for i in range(count):
        dev = Device(name=f"d{campaign_id}-{i}", status=DeviceStatus.IDLE)
        session.add(dev)
        await session.flush()
        sim = SimCard(
            iccid=f"898600{campaign_id:04d}{i:010d}",
            phone_number=f"139000{campaign_id:02d}{i:03d}",
        )
        session.add(sim)
        await session.flush()
        session.add_all([
            CampaignDevice(campaign_id=campaign_id, device_id=dev.id),
            DeviceSimBinding(
                device_id=dev.id, sim_card_id=sim.id, is_active=True
            ),
        ])


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

        # Seed 10 idle devices for camp_a (more than enough for the 3-cap tick)
        await _seed_devices(session, campaign_id=camp_a.id, count=10)

        await session.commit()
        a_id, b_id = camp_a.id, camp_b.id

    # Activate both campaigns
    active = ActiveCampaigns(redis_client)
    await active.add(a_id)
    await active.add(b_id)

    # Concurrency cap = 3 → only 3 leads of camp_a should dispatch this tick
    settings = _settings(max_conc=3)

    await tick(
        sessionmaker=sessionmaker_,
        redis=redis_client,
        active=active,
        settings=settings,
        now=now,
    )

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

    # 3 devices flipped to DIALING, 7 stayed IDLE
    async with sessionmaker_() as session:
        from sqlalchemy import select
        dialing = await session.execute(
            select(Device).where(Device.status == DeviceStatus.DIALING)
        )
        idle = await session.execute(
            select(Device).where(Device.status == DeviceStatus.IDLE)
        )
    assert len(dialing.scalars().all()) == 3
    assert len(idle.scalars().all()) == 7
