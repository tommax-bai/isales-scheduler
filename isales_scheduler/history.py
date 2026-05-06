"""Pack last-N call summaries into DialRequest history.

Spec: retry-followup § Scenario "历史摘要由 scheduler 注入";
      design.md Decision 10 (仅跟进通话查; LIMIT N 控 token).
"""

from __future__ import annotations

import logging

from isales_common.models.call_record import CallRecord
from isales_common.models.call_summary import CallSummary
from isales_common.models.lead import Lead
from isales_common.schemas.messages.dial import HistorySummary
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


async def pack_history(
    session: AsyncSession,
    lead: Lead,
    *,
    limit: int,
) -> list[HistorySummary]:
    """Return at most ``limit`` summaries for follow-up dials, newest first.

    Non-follow-up leads (``follow_up_count == 0``) get an empty list — the
    engine doesn't need history on first contact.
    """

    if lead.follow_up_count <= 0:
        return []

    stmt = (
        select(
            CallSummary.call_record_id,
            CallSummary.summary_text,
            CallRecord.ended_at,
        )
        .join(CallRecord, CallSummary.call_record_id == CallRecord.id)
        .where(CallRecord.lead_id == lead.id)
        .where(CallRecord.ended_at.is_not(None))
        .order_by(CallRecord.ended_at.desc())
        .limit(limit)
    )
    rows = (await session.execute(stmt)).all()

    out: list[HistorySummary] = []
    for call_record_id, summary_text, ended_at in rows:
        if summary_text is None or ended_at is None:
            continue
        out.append(
            HistorySummary(
                call_record_id=call_record_id,
                summary=summary_text,
                ended_at=ended_at.isoformat(),
            )
        )
    return out
