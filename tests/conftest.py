"""Shared test fixtures — real Postgres + Redis (skip if unreachable).

Same pattern as isales-api / isales-telephony.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio
from isales_common.models import Base
from isales_common.utils.redis import get_redis as common_redis
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

DEFAULT_TEST_URL = "postgresql+asyncpg://bears@localhost:5432/isales_scheduler_test"
DEFAULT_REDIS_URL = "redis://localhost:6379/0"


def _resolve_db_url() -> str:
    return (
        os.environ.get("ISALES_TEST_DATABASE_URL")
        or os.environ.get("ISALES_DATABASE_URL")
        or DEFAULT_TEST_URL
    )


def _resolve_redis_url() -> str:
    return os.environ.get("ISALES_REDIS_URL", DEFAULT_REDIS_URL)


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def engine() -> AsyncIterator[AsyncEngine]:
    eng = create_async_engine(_resolve_db_url(), future=True, poolclass=NullPool)
    try:
        async with eng.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)
    except Exception as exc:
        pytest.skip(f"PG not reachable: {exc}")
    yield eng
    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await eng.dispose()


@pytest_asyncio.fixture(loop_scope="session")
async def clean_engine(engine: AsyncEngine) -> AsyncIterator[AsyncEngine]:
    async with engine.begin() as conn:
        await conn.exec_driver_sql(
            "TRUNCATE campaign, role_config, lead, call_record, call_summary, "
            "holiday, prompt_version, device, sim_card, "
            "campaign_device, device_sim_binding "
            "RESTART IDENTITY CASCADE"
        )
    yield engine


@pytest_asyncio.fixture(loop_scope="session")
async def sessionmaker_(clean_engine: AsyncEngine) -> Any:
    return async_sessionmaker(clean_engine, expire_on_commit=False)


@pytest_asyncio.fixture(loop_scope="session")
async def redis_client() -> AsyncIterator[Any]:
    client = common_redis(_resolve_redis_url())
    try:
        await client.ping()
    except Exception as exc:
        pytest.skip(f"Redis not reachable: {exc}")
    # Clean keys we touch
    keys = [
        "scheduler:campaign-control",
        "scheduler:dlq",
        "scheduler:active-campaigns",
        "isales:concurrency:active",
        "engine:dial",
    ]
    for k in keys:
        await client.delete(k)
    yield client
    for k in keys:
        await client.delete(k)
    await client.aclose()
