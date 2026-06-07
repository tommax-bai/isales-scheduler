"""isales-scheduler entrypoint — wires control + scheduler loops.

Spec: design.md § Migration Plan;
      service-communication § 通信通道矩阵.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal

from isales_scheduler.control import ActiveCampaigns, control_loop
from isales_scheduler.db import get_engine, get_sessionmaker
from isales_scheduler.loop import scheduler_loop
from isales_scheduler.redis_client import get_redis
from isales_scheduler.settings import load_settings

logger = logging.getLogger(__name__)


async def _main() -> None:
    settings = load_settings()
    engine = get_engine(settings.database_url)
    sessionmaker = get_sessionmaker(engine)
    redis = get_redis(settings.redis_url)

    active = ActiveCampaigns(redis)
    await active.restore_from_redis()

    wake_event = asyncio.Event()
    stop_event = asyncio.Event()

    def _request_stop() -> None:
        logger.info("shutdown_signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            # add_signal_handler unavailable on some platforms (e.g. Windows).
            loop.add_signal_handler(sig, _request_stop)

    control_task = asyncio.create_task(
        control_loop(active, redis, wake_event), name="control_loop"
    )
    scheduler_task = asyncio.create_task(
        scheduler_loop(
            sessionmaker=sessionmaker,
            redis=redis,
            active=active,
            settings=settings,
            wake_event=wake_event,
        ),
        name="scheduler_loop",
    )

    logger.info("isales_scheduler_started")
    try:
        await stop_event.wait()
    finally:
        for task in (control_task, scheduler_task):
            task.cancel()
        for task in (control_task, scheduler_task):
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("task_shutdown_error name=%s", task.get_name())

        await redis.close()
        await engine.dispose()
        logger.info("isales_scheduler_stopped")


def run() -> None:
    """Console-script entry point: ``isales-scheduler``."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(_main())


if __name__ == "__main__":
    run()
