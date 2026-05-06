"""Tests for global concurrency counter — atomic INCR-with-cap + floored DECR."""

from __future__ import annotations

import asyncio

import pytest

from isales_scheduler import concurrency


@pytest.mark.asyncio(loop_scope="session")
async def test_increment_under_cap_returns_true(redis_client) -> None:  # type: ignore[no-untyped-def]
    assert await concurrency.try_increment(redis_client, max_concurrency=3) is True
    assert await concurrency.get(redis_client) == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_increment_at_cap_returns_false_and_does_not_change_value(redis_client) -> None:  # type: ignore[no-untyped-def]
    for _ in range(3):
        assert await concurrency.try_increment(redis_client, max_concurrency=3) is True
    assert await concurrency.get(redis_client) == 3
    assert await concurrency.try_increment(redis_client, max_concurrency=3) is False
    assert await concurrency.get(redis_client) == 3


@pytest.mark.asyncio(loop_scope="session")
async def test_decrement_after_acquire_releases_slot(redis_client) -> None:  # type: ignore[no-untyped-def]
    for _ in range(3):
        await concurrency.try_increment(redis_client, max_concurrency=3)
    assert await concurrency.try_increment(redis_client, max_concurrency=3) is False
    await concurrency.decrement(redis_client)
    assert await concurrency.try_increment(redis_client, max_concurrency=3) is True


@pytest.mark.asyncio(loop_scope="session")
async def test_decrement_floors_at_zero(redis_client) -> None:  # type: ignore[no-untyped-def]
    # Counter starts cleaned by fixture
    await concurrency.decrement(redis_client)
    await concurrency.decrement(redis_client)
    assert await concurrency.get(redis_client) == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_concurrent_acquire_only_max_succeed(redis_client) -> None:  # type: ignore[no-untyped-def]
    # 100 concurrent attempts, max=8 → exactly 8 should win
    cap = 8
    results = await asyncio.gather(
        *[concurrency.try_increment(redis_client, max_concurrency=cap) for _ in range(100)]
    )
    assert sum(1 for r in results if r) == cap
    assert await concurrency.get(redis_client) == cap
