"""Cloud-side device selection — direct PG query, no HTTP.

Spec: service-communication § Requirement "HTTP 调用规范"
      Scenario "scheduler 不再 HTTP 调 telephony-api"
      (cloud-edge 拆分后选 device 由云端 scheduler 基于云内 PG 查询完成).
      arch-cloud-edge-split design.md Decision 6 (telephony-api HTTP 角色降级).

Replaces the prior ``TelephonyClient.select_device`` HTTP call. Algorithm is
identical to the now-deprecated telephony-api ``POST /devices/select``:

    SELECT device.id, sim_card.phone_number
      FROM device
      JOIN campaign_device ON campaign_device.device_id = device.id
      JOIN device_sim_binding ON device_sim_binding.device_id = device.id
                              AND device_sim_binding.is_active = true
      JOIN sim_card ON sim_card.id = device_sim_binding.sim_card_id
     WHERE campaign_device.campaign_id = :cid
       AND device.status = 'idle'
     ORDER BY device.last_call_at ASC NULLS FIRST
     LIMIT 1
     FOR UPDATE OF device SKIP LOCKED;

    UPDATE device SET last_call_at = now(), status = 'dialing'
     WHERE id = :picked_id;

``SKIP LOCKED`` ensures concurrent dispatchers pick *different* idle devices
instead of contending on the same row. NULLS FIRST keeps never-called devices
ahead. Flipping status='dialing' makes the SELECT the authoritative
reservation point (without it two non-overlapping calls would re-pick the same
row since status stays 'idle'). The caller's transaction MUST commit for the
reservation to take effect.
"""

from __future__ import annotations

import logging

from isales_common.enums import DeviceStatus
from isales_common.models import (
    CampaignDevice,
    Device,
    DeviceSimBinding,
    SimCard,
)
from isales_common.schemas.device import DeviceSelectResponse
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.functions import now as sql_now

logger = logging.getLogger(__name__)


async def pick_idle_device(
    session: AsyncSession, campaign_id: int
) -> DeviceSelectResponse | None:
    """Pick + reserve the longest-idle device bound to ``campaign_id``.

    Returns ``None`` if no idle device is available (caller treats this as a
    transient failure: roll back concurrency, retry on next tick). Returns a
    ``DeviceSelectResponse`` with the chosen device_id and its active SIM's
    phone number on success. The device row is updated to ``status='dialing'``
    + ``last_call_at=now()`` within the caller's transaction.
    """

    stmt = (
        select(Device.id, SimCard.phone_number)
        .join(CampaignDevice, CampaignDevice.device_id == Device.id)
        .join(
            DeviceSimBinding,
            (DeviceSimBinding.device_id == Device.id)
            & (DeviceSimBinding.is_active.is_(True)),
        )
        .join(SimCard, SimCard.id == DeviceSimBinding.sim_card_id)
        .where(
            CampaignDevice.campaign_id == campaign_id,
            Device.status == DeviceStatus.IDLE,
        )
        .order_by(Device.last_call_at.asc().nulls_first())
        .limit(1)
        .with_for_update(skip_locked=True, of=Device)
    )
    row = (await session.execute(stmt)).first()
    if row is None:
        logger.info("pick_idle_device_no_match campaign_id=%d", campaign_id)
        return None

    device_id, phone_number = row
    if phone_number is None:
        logger.warning(
            "pick_idle_device_sim_no_number campaign_id=%d device_id=%d",
            campaign_id,
            device_id,
        )
        return None

    await session.execute(
        update(Device)
        .where(Device.id == device_id)
        .values(last_call_at=sql_now(), status=DeviceStatus.DIALING)
    )
    return DeviceSelectResponse(device_id=device_id, phone_number=phone_number)
