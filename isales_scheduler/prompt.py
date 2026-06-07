"""Snapshot prompt versions for a campaign at dispatch time.

Spec: retry-followup § Requirement "scheduler 调度数据流" step 6;
      role-prompt (snapshot semantics).

Reads ``role_config`` rows for the campaign, takes each enabled slot's
``current_prompt_version_id``, and shapes them into ``PromptVersionsSnapshot``.
"""

from __future__ import annotations

import logging

from isales_common.enums import RoleKind
from isales_common.models.role_config import RoleConfig
from isales_common.schemas.messages.dial import (
    PersonaPromptVersionRef,
    PromptVersionRef,
    PromptVersionsSnapshot,
    RefereePromptVersionRef,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


async def pack_prompt_versions(
    session: AsyncSession,
    campaign_id: int,
) -> PromptVersionsSnapshot:
    stmt = (
        select(RoleConfig)
        .where(RoleConfig.campaign_id == campaign_id)
        .where(RoleConfig.enabled.is_(True))
    )
    role_configs = list((await session.execute(stmt)).scalars().all())

    # engine-multi-referee-and-restructure: N referees (each tagged with its
    # routing label) + one main / restructure / extractor slot (last enabled row
    # of each singleton kind wins).
    main_ref: PromptVersionRef | None = None
    restructure_ref: PromptVersionRef | None = None
    extractor_ref: PromptVersionRef | None = None
    referee_refs: list[RefereePromptVersionRef] = []
    # engine-tools-multidialogue-gating: opt-in speculative dialogue personas,
    # packed like referees (label is the stable id routing rules bind to). The
    # persona label namespace is independent of the referee label namespace.
    persona_refs: list[PersonaPromptVersionRef] = []

    for rc in role_configs:
        if rc.current_prompt_version_id is None:
            continue
        ref = PromptVersionRef(
            role_config_id=rc.id,
            prompt_version_id=rc.current_prompt_version_id,
        )
        if rc.kind == RoleKind.MAIN:
            main_ref = ref
        elif rc.kind == RoleKind.REFEREE:
            referee_refs.append(
                RefereePromptVersionRef(
                    role_config_id=rc.id,
                    prompt_version_id=rc.current_prompt_version_id,
                    label=rc.label,
                )
            )
        elif rc.kind == RoleKind.PERSONA:
            persona_refs.append(
                PersonaPromptVersionRef(
                    role_config_id=rc.id,
                    prompt_version_id=rc.current_prompt_version_id,
                    label=rc.label,
                )
            )
        elif rc.kind == RoleKind.RESTRUCTURE:
            restructure_ref = ref
        elif rc.kind == RoleKind.EXTRACTOR:
            extractor_ref = ref

    return PromptVersionsSnapshot(
        main_llm=main_ref,
        referee_llms=referee_refs,
        persona_llms=persona_refs,
        restructure_llm=restructure_ref,
        extractor_llm=extractor_ref,
        wrap_up_appended=False,
    )
