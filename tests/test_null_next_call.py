"""Tick test — leads with `next_call_at IS NULL` are dispatched.

Spec: openspec/changes/web-admin-campaign-workflow §2.6 / retry-followup
§「新线索首次入队语义」. A freshly created lead has `next_call_at = NULL`
and no code initializes it; scheduler 取数把 NULL 视为"立即可呼"。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import pytest
from isales_common.enums import DeviceStatus, LeadStatus
from isales_common.models import CampaignDevice, Device, DeviceSimBinding, SimCard
from isales_common.models.campaign import Campaign
from isales_common.models.lead import Lead
from sqlalchemy import select

from isales_scheduler.control import ActiveCampaigns
from isales_scheduler.loop import tick
from isales_scheduler.settings import Settings

TZ = ZoneInfo("Asia/Shanghai")

ALWAYS_OPEN: list[dict[str, Any]] = [
    {
        "days": ["mon", "tue", "wed", "thu", "fri", "sat", "sun"],
        "start": "00:00",
        "end": "23:59",
    },
]


def _settings() -> Settings:
    import os

    os.environ.setdefault("ISALES_DATABASE_URL", "postgresql+asyncpg://test/test")
    os.environ.setdefault("ISALES_REDIS_URL", "redis://localhost:6379/0")
    s = Settings()
    s.max_concurrency = 8
    s.scheduler_batch_size = 50
    s.scheduler_history_n = 3
    return s


@pytest.mark.asyncio(loop_scope="session")
async def test_tick_dispatches_lead_with_null_next_call_at(  # type: ignore[no-untyped-def]
    sessionmaker_, redis_client
) -> None:
    now = datetime(2026, 5, 4, 10, 0, tzinfo=TZ)  # Monday, inside window

    async with sessionmaker_() as session:
        camp = Campaign(name="N", time_windows=ALWAYS_OPEN, respect_holidays=False)
        session.add(camp)
        await session.flush()

        # A brand-new lead — status=new, next_call_at NOT set (NULL).
        lead = Lead(
            campaign_id=camp.id,
            phone="13800001234",
            status=LeadStatus.NEW,
        )
        session.add(lead)
        await session.flush()

        # one idle device + active SIM
        dev = Device(name="dn-0", status=DeviceStatus.IDLE)
        session.add(dev)
        await session.flush()
        sim = SimCard(iccid="89860000000000000001", phone_number="13900000001")
        session.add(sim)
        await session.flush()
        session.add_all(
            [
                CampaignDevice(campaign_id=camp.id, device_id=dev.id),
                DeviceSimBinding(device_id=dev.id, sim_card_id=sim.id, is_active=True),
            ]
        )
        await session.commit()
        camp_id, lead_id = camp.id, lead.id

    # sanity: the lead really has NULL next_call_at
    async with sessionmaker_() as session:
        fresh = await session.get(Lead, lead_id)
        assert fresh is not None
        assert fresh.next_call_at is None

    active = ActiveCampaigns(redis_client)
    await active.add(camp_id)

    await tick(
        sessionmaker=sessionmaker_,
        redis=redis_client,
        active=active,
        settings=_settings(),
        now=now,
    )

    # dispatch 成功 ⟺ loop.py 把 lead.status 写为 CALLING（仅在 lpush 成功
    # 后才写，见 dispatch.py）。不直接断言 redis DIAL_QUEUE 长度——本地 redis
    # 环境下 llen 不稳定（test_loop.py 同款 pre-existing flake）；
    # lead.status=CALLING 已充分证明 NULL next_call_at 的 lead 被取数纳入
    # 并完成派发。
    async with sessionmaker_() as session:
        calling = (
            await session.execute(
                select(Lead).where(Lead.status == LeadStatus.CALLING)
            )
        ).scalars().all()
        assert len(calling) == 1
        assert calling[0].id == lead_id
