"""Tests for CampaignControl consumption + active set + DLQ."""

from __future__ import annotations

import asyncio
import contextlib

import pytest
from isales_common.schemas.messages import (
    PauseCampaign,
    ResumeCampaign,
    StartCampaign,
)

from isales_scheduler.control import (
    ACTIVE_SET,
    CONTROL_QUEUE,
    DLQ,
    ActiveCampaigns,
    _handle_message,
    control_loop,
)


@pytest.mark.asyncio(loop_scope="session")
async def test_active_campaigns_add_remove_round_trip(redis_client) -> None:  # type: ignore[no-untyped-def]
    ac = ActiveCampaigns(redis_client)
    await ac.add(101)
    await ac.add(102)
    assert await ac.snapshot() == {101, 102}
    await ac.remove(101)
    assert await ac.snapshot() == {102}
    # Redis SET should match
    members = {int(m) for m in await redis_client.smembers(ACTIVE_SET)}
    assert members == {102}


@pytest.mark.asyncio(loop_scope="session")
async def test_restore_from_redis_rebuilds_cache(redis_client) -> None:  # type: ignore[no-untyped-def]
    await redis_client.sadd(ACTIVE_SET, 7, 11, 13)
    ac = ActiveCampaigns(redis_client)
    await ac.restore_from_redis()
    assert await ac.snapshot() == {7, 11, 13}


@pytest.mark.asyncio(loop_scope="session")
async def test_handle_start_resume_pause(redis_client) -> None:  # type: ignore[no-untyped-def]
    ac = ActiveCampaigns(redis_client)
    wake_event = asyncio.Event()

    await _handle_message(
        StartCampaign(campaign_id=42).model_dump_json(), ac, redis_client, wake_event
    )
    assert await ac.snapshot() == {42}
    assert wake_event.is_set()
    wake_event.clear()

    await _handle_message(
        ResumeCampaign(campaign_id=43).model_dump_json(), ac, redis_client, wake_event
    )
    assert await ac.snapshot() == {42, 43}
    assert wake_event.is_set()
    wake_event.clear()

    await _handle_message(
        PauseCampaign(campaign_id=42).model_dump_json(), ac, redis_client, wake_event
    )
    assert await ac.snapshot() == {43}
    assert not wake_event.is_set()


@pytest.mark.asyncio(loop_scope="session")
async def test_unsupported_schema_version_lands_in_dlq(redis_client) -> None:  # type: ignore[no-untyped-def]
    ac = ActiveCampaigns(redis_client)
    bad_raw = '{"schema_version": 99, "type": "start", "campaign_id": 1}'
    await _handle_message(bad_raw, ac, redis_client, asyncio.Event())
    assert await ac.snapshot() == set()
    assert await redis_client.llen(DLQ) == 1
    raw = await redis_client.lrange(DLQ, 0, 0)
    assert raw[0] == bad_raw


@pytest.mark.asyncio(loop_scope="session")
async def test_malformed_json_lands_in_dlq(redis_client) -> None:  # type: ignore[no-untyped-def]
    ac = ActiveCampaigns(redis_client)
    await _handle_message("not even json", ac, redis_client, asyncio.Event())
    assert await redis_client.llen(DLQ) == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_validation_error_with_supported_schema_lands_in_dlq(redis_client) -> None:  # type: ignore[no-untyped-def]
    ac = ActiveCampaigns(redis_client)
    # supported schema_version=1 but unknown discriminator type
    bad_raw = '{"schema_version": 1, "type": "fly", "campaign_id": 1}'
    await _handle_message(bad_raw, ac, redis_client, asyncio.Event())
    assert await redis_client.llen(DLQ) == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_control_loop_consumes_blpop_and_exits_on_cancel(redis_client) -> None:  # type: ignore[no-untyped-def]
    ac = ActiveCampaigns(redis_client)
    msg = StartCampaign(campaign_id=999).model_dump_json()
    await redis_client.lpush(CONTROL_QUEUE, msg)

    wake_event = asyncio.Event()
    task = asyncio.create_task(control_loop(ac, redis_client, wake_event))
    # Give the loop a moment to consume.
    for _ in range(20):
        await asyncio.sleep(0.05)
        if 999 in (await ac.snapshot()):
            break
    assert 999 in (await ac.snapshot())

    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
