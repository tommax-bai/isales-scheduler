"""Integration tests for dispatch_lead — end-to-end with real PG + Redis,
mocked telephony-api via httpx MockTransport.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import pytest
from isales_common.enums import LeadStatus
from isales_common.models.campaign import Campaign
from isales_common.models.lead import Lead
from isales_common.schemas.messages.dial import DialRequest

from isales_scheduler import concurrency
from isales_scheduler.dispatch import DIAL_QUEUE, dispatch_lead
from isales_scheduler.telephony import TelephonyClient

TZ = ZoneInfo("Asia/Shanghai")


WORKDAY_WINDOWS: list[dict[str, Any]] = [
    {"days": ["mon", "tue", "wed", "thu", "fri"], "start": "00:00", "end": "23:59"},
]


def _client_with_handler(handler):  # type: ignore[no-untyped-def]
    transport = httpx.MockTransport(handler)
    c = TelephonyClient("http://t.test", timeout=1.0)
    c._client = httpx.AsyncClient(transport=transport, timeout=1.0)
    return c


async def _seed(sessionmaker_, *, time_windows=WORKDAY_WINDOWS) -> tuple[int, int]:  # type: ignore[no-untyped-def]
    async with sessionmaker_() as session:
        camp = Campaign(name="C", time_windows=time_windows, respect_holidays=False)
        session.add(camp)
        await session.flush()
        lead = Lead(
            campaign_id=camp.id,
            phone="13800000000",
            status=LeadStatus.NEW,
            next_call_at=datetime(2026, 5, 4, 10, 0, tzinfo=TZ),
        )
        session.add(lead)
        await session.commit()
        return camp.id, lead.id


@pytest.mark.asyncio(loop_scope="session")
async def test_dispatch_success_pushes_message_and_marks_calling(  # type: ignore[no-untyped-def]
    sessionmaker_, redis_client
) -> None:
    camp_id, lead_id = await _seed(sessionmaker_)

    def handler(_req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"device_id": 7, "phone_number": "13900000000"})

    telephony = _client_with_handler(handler)
    now = datetime(2026, 5, 4, 10, 0, tzinfo=TZ)

    async with sessionmaker_() as session:
        camp = await session.get(Campaign, camp_id)
        lead = await session.get(Lead, lead_id)
        ok = await dispatch_lead(
            session=session,
            redis=redis_client,
            telephony=telephony,
            campaign=camp,
            lead=lead,
            now=now,
            holiday_dates=set(),
            max_concurrency=8,
            history_n=3,
        )
        await session.commit()

    await telephony.aclose()
    assert ok is True

    # Lead status flipped to calling
    async with sessionmaker_() as session:
        lead2 = await session.get(Lead, lead_id)
        assert lead2.status == LeadStatus.CALLING

    # DialRequest in queue, schema-valid
    raw = await redis_client.rpop(DIAL_QUEUE)
    assert raw is not None
    msg = DialRequest.model_validate_json(raw)
    assert msg.lead.lead_id == lead_id
    assert msg.lead.campaign_id == camp_id
    assert msg.caller_id == "13900000000"
    assert msg.device_id == 7

    # Concurrency was incremented (and not rolled back)
    assert await concurrency.get(redis_client) == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_dispatch_device_select_failure_rolls_back_and_keeps_status(  # type: ignore[no-untyped-def]
    sessionmaker_, redis_client
) -> None:
    camp_id, lead_id = await _seed(sessionmaker_)

    def handler(_req: httpx.Request) -> httpx.Response:
        return httpx.Response(503)

    telephony = _client_with_handler(handler)
    now = datetime(2026, 5, 4, 10, 0, tzinfo=TZ)

    async with sessionmaker_() as session:
        camp = await session.get(Campaign, camp_id)
        lead = await session.get(Lead, lead_id)
        ok = await dispatch_lead(
            session=session,
            redis=redis_client,
            telephony=telephony,
            campaign=camp,
            lead=lead,
            now=now,
            holiday_dates=set(),
            max_concurrency=8,
            history_n=3,
        )
        await session.commit()

    await telephony.aclose()
    assert ok is False

    async with sessionmaker_() as session:
        lead2 = await session.get(Lead, lead_id)
        assert lead2.status == LeadStatus.NEW  # unchanged

    assert await redis_client.llen(DIAL_QUEUE) == 0
    assert await concurrency.get(redis_client) == 0  # rolled back


@pytest.mark.asyncio(loop_scope="session")
async def test_dispatch_concurrency_full_skips(sessionmaker_, redis_client) -> None:  # type: ignore[no-untyped-def]
    camp_id, lead_id = await _seed(sessionmaker_)

    # Pre-fill counter to cap
    for _ in range(8):
        await concurrency.try_increment(redis_client, max_concurrency=8)

    def handler(_req: httpx.Request) -> httpx.Response:
        raise AssertionError("device_select should not be called")

    telephony = _client_with_handler(handler)
    now = datetime(2026, 5, 4, 10, 0, tzinfo=TZ)

    async with sessionmaker_() as session:
        camp = await session.get(Campaign, camp_id)
        lead = await session.get(Lead, lead_id)
        ok = await dispatch_lead(
            session=session,
            redis=redis_client,
            telephony=telephony,
            campaign=camp,
            lead=lead,
            now=now,
            holiday_dates=set(),
            max_concurrency=8,
            history_n=3,
        )
        await session.commit()

    await telephony.aclose()
    assert ok is False
    async with sessionmaker_() as session:
        lead2 = await session.get(Lead, lead_id)
        assert lead2.status == LeadStatus.NEW
    assert await concurrency.get(redis_client) == 8


@pytest.mark.asyncio(loop_scope="session")
async def test_dispatch_out_of_window_defers_next_call_at(sessionmaker_, redis_client) -> None:  # type: ignore[no-untyped-def]
    # Window = mon 09-12 only; lead.next_call_at noon on Mon → out of window
    windows: list[dict[str, Any]] = [
        {"days": ["mon"], "start": "09:00", "end": "12:00"},
    ]
    async with sessionmaker_() as session:
        camp = Campaign(name="W", time_windows=windows, respect_holidays=False)
        session.add(camp)
        await session.flush()
        lead = Lead(
            campaign_id=camp.id,
            phone="13800000099",
            status=LeadStatus.NEW,
            next_call_at=datetime(2026, 5, 4, 14, 0, tzinfo=TZ),
        )
        session.add(lead)
        await session.commit()
        camp_id, lead_id = camp.id, lead.id

    def handler(_req: httpx.Request) -> httpx.Response:
        raise AssertionError("device_select should not be called out-of-window")

    telephony = _client_with_handler(handler)
    now = datetime(2026, 5, 4, 14, 0, tzinfo=TZ)

    async with sessionmaker_() as session:
        camp = await session.get(Campaign, camp_id)
        lead = await session.get(Lead, lead_id)
        ok = await dispatch_lead(
            session=session,
            redis=redis_client,
            telephony=telephony,
            campaign=camp,
            lead=lead,
            now=now,
            holiday_dates=set(),
            max_concurrency=8,
            history_n=3,
        )
        await session.commit()

    await telephony.aclose()
    assert ok is False

    async with sessionmaker_() as session:
        lead2 = await session.get(Lead, lead_id)
        # Should be deferred to next Mon 09:00
        assert lead2.status == LeadStatus.NEW
        assert lead2.next_call_at is not None
        next_dt = lead2.next_call_at.astimezone(TZ)
        assert next_dt.hour == 9
        assert next_dt.weekday() == 0
        assert next_dt > now
    assert await concurrency.get(redis_client) == 0
