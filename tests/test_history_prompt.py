"""Tests for history packing + prompt-version snapshot."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from isales_common.enums import LeadStatus, RoleKind
from isales_common.models.call_record import CallRecord
from isales_common.models.call_summary import CallSummary
from isales_common.models.campaign import Campaign
from isales_common.models.lead import Lead
from isales_common.models.prompt import PromptVersion
from isales_common.models.role_config import RoleConfig

from isales_scheduler.history import pack_history
from isales_scheduler.prompt import pack_prompt_versions


@pytest.mark.asyncio(loop_scope="session")
async def test_pack_history_takes_last_n_for_followup(sessionmaker_) -> None:  # type: ignore[no-untyped-def]
    async with sessionmaker_() as session:
        camp = Campaign(name="C")
        session.add(camp)
        await session.flush()
        lead = Lead(
            campaign_id=camp.id,
            phone="13800000000",
            status=LeadStatus.FOLLOWING_UP,
            follow_up_count=2,
        )
        session.add(lead)
        await session.flush()

        base = datetime(2026, 5, 1, 10, 0, tzinfo=UTC)
        for i in range(5):
            cr = CallRecord(
                lead_id=lead.id,
                campaign_id=camp.id,
                ended_at=base + timedelta(days=i),
            )
            session.add(cr)
            await session.flush()
            session.add(
                CallSummary(call_record_id=cr.id, summary_text=f"summary-{i}")
            )
        await session.commit()

        out = await pack_history(session, lead, limit=3)

    assert len(out) == 3
    # Newest first — the i=4 row is newest
    assert out[0].summary == "summary-4"
    assert out[1].summary == "summary-3"
    assert out[2].summary == "summary-2"


@pytest.mark.asyncio(loop_scope="session")
async def test_pack_history_empty_for_non_followup_lead(sessionmaker_) -> None:  # type: ignore[no-untyped-def]
    async with sessionmaker_() as session:
        camp = Campaign(name="C2")
        session.add(camp)
        await session.flush()
        lead = Lead(
            campaign_id=camp.id,
            phone="13800000001",
            status=LeadStatus.NEW,
            follow_up_count=0,
        )
        session.add(lead)
        await session.commit()

        out = await pack_history(session, lead, limit=3)
    assert out == []


@pytest.mark.asyncio(loop_scope="session")
async def test_pack_prompt_versions_picks_active_per_kind(sessionmaker_) -> None:  # type: ignore[no-untyped-def]
    async with sessionmaker_() as session:
        camp = Campaign(name="C3")
        session.add(camp)
        await session.flush()

        # Create role_configs first with NULL current_prompt_version_id;
        # then create prompt_versions and update.
        role = RoleConfig(campaign_id=camp.id, kind=RoleKind.ROLE, model="gpt-4")
        judge = RoleConfig(campaign_id=camp.id, kind=RoleKind.JUDGE, model="gpt-4")
        polish = RoleConfig(campaign_id=camp.id, kind=RoleKind.POLISH, model="gpt-4")
        disabled_role = RoleConfig(
            campaign_id=camp.id, kind=RoleKind.ROLE, model="gpt-4", enabled=False
        )
        session.add_all([role, judge, polish, disabled_role])
        await session.flush()

        from isales_common.enums import PromptScopeType

        pv_role = PromptVersion(
            scope_type=PromptScopeType.ROLE, scope_id=role.id, content="role", is_active=True
        )
        pv_judge = PromptVersion(
            scope_type=PromptScopeType.JUDGE, scope_id=judge.id, content="judge", is_active=True
        )
        pv_polish = PromptVersion(
            scope_type=PromptScopeType.POLISH, scope_id=polish.id,
            content="polish", is_active=True,
        )
        session.add_all([pv_role, pv_judge, pv_polish])
        await session.flush()

        role.current_prompt_version_id = pv_role.id
        judge.current_prompt_version_id = pv_judge.id
        polish.current_prompt_version_id = pv_polish.id
        # disabled_role intentionally has no prompt — should be skipped anyway
        await session.commit()

        snapshot = await pack_prompt_versions(session, camp.id)

    assert len(snapshot.role_llms) == 1
    assert snapshot.role_llms[0].role_config_id == role.id
    assert snapshot.role_llms[0].prompt_version_id == pv_role.id
    assert snapshot.judge_llm is not None
    assert snapshot.judge_llm.prompt_version_id == pv_judge.id
    assert snapshot.polish_llm is not None
    assert snapshot.polish_llm.prompt_version_id == pv_polish.id
    assert snapshot.wrap_up_appended is False
