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
        main = RoleConfig(campaign_id=camp.id, kind=RoleKind.MAIN, model="gpt-4")
        referee = RoleConfig(campaign_id=camp.id, kind=RoleKind.REFEREE, model="gpt-4")
        extractor = RoleConfig(campaign_id=camp.id, kind=RoleKind.EXTRACTOR, model="gpt-4")
        disabled_main = RoleConfig(
            campaign_id=camp.id, kind=RoleKind.MAIN, model="gpt-4", enabled=False
        )
        session.add_all([main, referee, extractor, disabled_main])
        await session.flush()

        from isales_common.enums import PromptScopeType

        pv_main = PromptVersion(
            scope_type=PromptScopeType.MAIN, scope_id=main.id, content="main", is_active=True
        )
        pv_referee = PromptVersion(
            scope_type=PromptScopeType.REFEREE, scope_id=referee.id,
            content="referee", is_active=True,
        )
        pv_extractor = PromptVersion(
            scope_type=PromptScopeType.EXTRACTOR, scope_id=extractor.id,
            content="extractor", is_active=True,
        )
        session.add_all([pv_main, pv_referee, pv_extractor])
        await session.flush()

        main.current_prompt_version_id = pv_main.id
        referee.current_prompt_version_id = pv_referee.id
        extractor.current_prompt_version_id = pv_extractor.id
        # disabled_main intentionally has no prompt — should be skipped anyway
        await session.commit()

        snapshot = await pack_prompt_versions(session, camp.id)

    assert snapshot.main_llm is not None
    assert snapshot.main_llm.role_config_id == main.id
    assert snapshot.main_llm.prompt_version_id == pv_main.id
    assert len(snapshot.referee_llms) == 1
    assert snapshot.referee_llms[0].prompt_version_id == pv_referee.id
    assert snapshot.extractor_llm is not None
    assert snapshot.extractor_llm.prompt_version_id == pv_extractor.id
    assert snapshot.wrap_up_appended is False
    # no personas configured → empty list (backward compatible)
    assert snapshot.persona_llms == []


async def test_pack_prompt_versions_includes_personas(sessionmaker_) -> None:  # type: ignore[no-untyped-def]
    # engine-tools-multidialogue-gating: kind=persona rows pack into persona_llms,
    # tagged with their label (independent of the referee label namespace).
    async with sessionmaker_() as session:
        from isales_common.enums import PromptScopeType

        camp = Campaign(name="C-persona")
        session.add(camp)
        await session.flush()

        main = RoleConfig(campaign_id=camp.id, kind=RoleKind.MAIN, model="gpt-4")
        # a referee and a persona that happen to share the label "warm" — must
        # NOT collide (addressed by kind + label).
        referee = RoleConfig(
            campaign_id=camp.id, kind=RoleKind.REFEREE, model="gpt-4", label="warm"
        )
        persona = RoleConfig(
            campaign_id=camp.id, kind=RoleKind.PERSONA, model="gpt-4", label="warm"
        )
        session.add_all([main, referee, persona])
        await session.flush()

        pv_main = PromptVersion(
            scope_type=PromptScopeType.MAIN, scope_id=main.id, content="m", is_active=True
        )
        pv_referee = PromptVersion(
            scope_type=PromptScopeType.REFEREE, scope_id=referee.id, content="r", is_active=True
        )
        pv_persona = PromptVersion(
            scope_type=PromptScopeType.PERSONA, scope_id=persona.id, content="p", is_active=True
        )
        session.add_all([pv_main, pv_referee, pv_persona])
        await session.flush()

        main.current_prompt_version_id = pv_main.id
        referee.current_prompt_version_id = pv_referee.id
        persona.current_prompt_version_id = pv_persona.id
        await session.commit()

        snapshot = await pack_prompt_versions(session, camp.id)

    assert len(snapshot.persona_llms) == 1
    assert snapshot.persona_llms[0].label == "warm"
    assert snapshot.persona_llms[0].prompt_version_id == pv_persona.id
    # referee packing unchanged + label namespace isolated
    assert len(snapshot.referee_llms) == 1
    assert snapshot.referee_llms[0].label == "warm"
    assert snapshot.referee_llms[0].prompt_version_id == pv_referee.id
