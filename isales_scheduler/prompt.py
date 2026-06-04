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
    PromptVersionRef,
    PromptVersionsSnapshot,
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

    # pipeline-stream-and-referee: dual-LLM snapshot — exactly one main /
    # referee / extractor slot (last enabled row of each kind wins).
    main_ref: PromptVersionRef | None = None
    referee_ref: PromptVersionRef | None = None
    extractor_ref: PromptVersionRef | None = None

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
            referee_ref = ref
        elif rc.kind == RoleKind.EXTRACTOR:
            extractor_ref = ref

    return PromptVersionsSnapshot(
        main_llm=main_ref,
        referee_llm=referee_ref,
        extractor_llm=extractor_ref,
        wrap_up_appended=False,
    )
