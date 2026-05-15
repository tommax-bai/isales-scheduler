"""Unit + concurrency tests for ``pick_idle_device``.

Spec: service-communication § Scenario "scheduler 不再 HTTP 调 telephony-api";
      arch-cloud-edge-split tasks.md § 6.4 (并发安全：两 dial 不能选同一 idle device).
"""

from __future__ import annotations

import asyncio

import pytest
from isales_common.enums import DeviceStatus
from isales_common.models import (
    CampaignDevice,
    Device,
    DeviceSimBinding,
    SimCard,
)
from isales_common.models.campaign import Campaign

from isales_scheduler.device_selection import pick_idle_device


async def _seed_campaign_with_devices(  # type: ignore[no-untyped-def]
    sessionmaker_, *, count: int, all_active_sim: bool = True
) -> int:
    """Create one campaign + ``count`` idle devices, each with an active SIM."""
    async with sessionmaker_() as session:
        camp = Campaign(
            name="T",
            time_windows=[{"days": ["mon"], "start": "00:00", "end": "23:59"}],
            respect_holidays=False,
        )
        session.add(camp)
        await session.flush()
        for i in range(count):
            dev = Device(name=f"d{i}", status=DeviceStatus.IDLE)
            session.add(dev)
            await session.flush()
            sim = SimCard(
                iccid=f"8986{i:028d}",
                phone_number=f"139{i:08d}",
            )
            session.add(sim)
            await session.flush()
            session.add_all([
                CampaignDevice(campaign_id=camp.id, device_id=dev.id),
                DeviceSimBinding(
                    device_id=dev.id,
                    sim_card_id=sim.id,
                    is_active=all_active_sim,
                ),
            ])
        await session.commit()
        return camp.id


@pytest.mark.asyncio(loop_scope="session")
async def test_pick_returns_response_and_flips_device_status(sessionmaker_) -> None:  # type: ignore[no-untyped-def]
    camp_id = await _seed_campaign_with_devices(sessionmaker_, count=1)

    async with sessionmaker_() as session:
        resp = await pick_idle_device(session, camp_id)
        await session.commit()
    assert resp is not None
    assert resp.device_id >= 1
    assert resp.phone_number.startswith("139")

    async with sessionmaker_() as session:
        from sqlalchemy import select
        dev = (await session.execute(select(Device).limit(1))).scalar_one()
        assert dev.status == DeviceStatus.DIALING
        assert dev.last_call_at is not None


@pytest.mark.asyncio(loop_scope="session")
async def test_pick_returns_none_when_no_idle_device(sessionmaker_) -> None:  # type: ignore[no-untyped-def]
    # Campaign with no bound devices
    async with sessionmaker_() as session:
        camp = Campaign(
            name="empty",
            time_windows=[{"days": ["mon"], "start": "00:00", "end": "23:59"}],
            respect_holidays=False,
        )
        session.add(camp)
        await session.commit()
        camp_id = camp.id

    async with sessionmaker_() as session:
        resp = await pick_idle_device(session, camp_id)
    assert resp is None


@pytest.mark.asyncio(loop_scope="session")
async def test_pick_skips_inactive_binding(sessionmaker_) -> None:  # type: ignore[no-untyped-def]
    camp_id = await _seed_campaign_with_devices(
        sessionmaker_, count=1, all_active_sim=False
    )
    async with sessionmaker_() as session:
        resp = await pick_idle_device(session, camp_id)
    assert resp is None


@pytest.mark.asyncio(loop_scope="session")
async def test_pick_concurrent_callers_get_different_devices(  # type: ignore[no-untyped-def]
    sessionmaker_,
) -> None:
    """Two parallel transactions MUST pick different devices (SKIP LOCKED)."""

    camp_id = await _seed_campaign_with_devices(sessionmaker_, count=2)

    async def _one_pick() -> int | None:
        async with sessionmaker_() as session:
            resp = await pick_idle_device(session, camp_id)
            if resp is None:
                await session.rollback()
                return None
            await session.commit()
            return resp.device_id

    r1, r2 = await asyncio.gather(_one_pick(), _one_pick())
    assert r1 is not None
    assert r2 is not None
    assert r1 != r2

    # Both devices flipped to DIALING
    async with sessionmaker_() as session:
        from sqlalchemy import select
        rows = (
            await session.execute(select(Device.status))
        ).scalars().all()
        assert set(rows) == {DeviceStatus.DIALING}


@pytest.mark.asyncio(loop_scope="session")
async def test_pick_order_nulls_first_then_oldest_last_call(sessionmaker_) -> None:  # type: ignore[no-untyped-def]
    """Devices never called (NULLS FIRST) come before those with last_call_at set.

    Among called devices, oldest last_call_at wins.
    """
    from datetime import UTC, datetime, timedelta

    async with sessionmaker_() as session:
        camp = Campaign(
            name="order",
            time_windows=[{"days": ["mon"], "start": "00:00", "end": "23:59"}],
            respect_holidays=False,
        )
        session.add(camp)
        await session.flush()

        # Device A: last_call_at = 1h ago → should be picked second
        dev_a = Device(
            name="A",
            status=DeviceStatus.IDLE,
            last_call_at=datetime.now(UTC) - timedelta(hours=1),
        )
        # Device B: never called → picked first by NULLS FIRST
        dev_b = Device(name="B", status=DeviceStatus.IDLE)
        # Device C: last_call_at = 10m ago → never reached in this test
        dev_c = Device(
            name="C",
            status=DeviceStatus.IDLE,
            last_call_at=datetime.now(UTC) - timedelta(minutes=10),
        )
        session.add_all([dev_a, dev_b, dev_c])
        await session.flush()

        for dev, idx in [(dev_a, 0), (dev_b, 1), (dev_c, 2)]:
            sim = SimCard(
                iccid=f"8986{idx:028d}",
                phone_number=f"139{idx:08d}",
            )
            session.add(sim)
            await session.flush()
            session.add_all([
                CampaignDevice(campaign_id=camp.id, device_id=dev.id),
                DeviceSimBinding(
                    device_id=dev.id, sim_card_id=sim.id, is_active=True
                ),
            ])
        await session.commit()
        camp_id = camp.id
        b_id = dev_b.id
        a_id = dev_a.id

    async with sessionmaker_() as session:
        resp1 = await pick_idle_device(session, camp_id)
        await session.commit()
    assert resp1 is not None and resp1.device_id == b_id  # nulls first

    async with sessionmaker_() as session:
        resp2 = await pick_idle_device(session, camp_id)
        await session.commit()
    assert resp2 is not None and resp2.device_id == a_id  # then oldest last_call_at
