"""Mock engine consumer for stage-3 dev verification.

BRPOPs ``engine:dial``, validates each ``DialRequest`` schema, optionally
simulates call completion (decrements concurrency + resets lead so the next
tick re-dispatches). Not exposed via [project.scripts] — invoke as
``python -m scripts.fake_engine_consumer``.

WARNING: dev/test only. Not for production.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import UTC, datetime, timedelta

from isales_common.enums import LeadStatus
from isales_common.models.lead import Lead
from isales_common.schemas.messages.dial import DialRequest
from sqlalchemy import update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from isales_scheduler import concurrency
from isales_scheduler.redis_client import get_redis

DIAL_QUEUE = "engine:dial"

logger = logging.getLogger("fake_engine_consumer")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Mock engine consumer (dev only).")
    p.add_argument("--redis-url", required=True)
    p.add_argument("--db-url", required=False, default=None,
                   help="postgres async URL — required for simulate-call-end mode")
    p.add_argument("--rate-hz", type=float, default=1.0,
                   help="max messages per second to drain")
    p.add_argument("--max-messages", type=int, default=0,
                   help="stop after N messages (0 = unbounded)")
    p.add_argument("--ack-mode", choices=("drop", "simulate-call-end"), default="drop")
    p.add_argument("--reset-delay-seconds", type=int, default=5,
                   help="for simulate-call-end mode: lead.next_call_at = now + this")
    return p.parse_args()


async def _simulate_call_end(
    sessionmaker: async_sessionmaker,  # type: ignore[type-arg]
    redis,  # type: ignore[no-untyped-def]
    lead_id: int,
    *,
    delay_seconds: int,
) -> None:
    new_next = datetime.now(UTC) + timedelta(seconds=delay_seconds)
    async with sessionmaker() as session:
        await session.execute(
            update(Lead)
            .where(Lead.id == lead_id)
            .values(status=LeadStatus.NEW, next_call_at=new_next)
        )
        await session.commit()
    await concurrency.decrement(redis)


async def _run() -> None:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    redis = get_redis(args.redis_url)
    sessionmaker = None
    if args.ack_mode == "simulate-call-end":
        if not args.db_url:
            raise SystemExit("simulate-call-end mode requires --db-url")
        engine = create_async_engine(args.db_url, future=True)
        sessionmaker = async_sessionmaker(engine, expire_on_commit=False)

    delay = 1.0 / args.rate_hz if args.rate_hz > 0 else 0.0
    count = 0

    try:
        while True:
            popped = await redis.brpop([DIAL_QUEUE], timeout=2)
            if popped is None:
                continue
            _key, raw = popped
            try:
                msg = DialRequest.model_validate_json(raw)
            except Exception as exc:
                logger.error("decode_error err=%r raw=%s", exc, raw[:200])
                # Surface schema drift loudly — exit so CI / soak-test catches it.
                raise

            count += 1
            logger.info(
                "consumed message_id=%s lead_id=%d campaign_id=%d phone=%s caller_id=%s",
                msg.message_id, msg.lead.lead_id, msg.lead.campaign_id,
                msg.lead.phone, msg.caller_id,
            )

            if args.ack_mode == "simulate-call-end" and sessionmaker is not None:
                try:
                    await _simulate_call_end(
                        sessionmaker, redis, msg.lead.lead_id,
                        delay_seconds=args.reset_delay_seconds,
                    )
                except Exception:
                    logger.exception("simulate_call_end_failed lead_id=%d", msg.lead.lead_id)

            if args.max_messages and count >= args.max_messages:
                logger.info("max_messages_reached count=%d", count)
                break
            if delay:
                await asyncio.sleep(delay)
    finally:
        await redis.close()


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
