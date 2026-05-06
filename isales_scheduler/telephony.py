"""HTTP client for ``POST {telephony-api}/devices/select``.

Spec: service-communication § Requirement "HTTP 调用规范" (v1 内部 HTTP 仅
      scheduler → telephony-api);
      architecture § Scenario "服务间内部调用免 JWT" (v1 loopback);
      design.md Decision 9 (短超时 + 不重试 + 失败留下次 tick 自然重试).
"""

from __future__ import annotations

import logging

import httpx
from isales_common.schemas.device import DeviceSelectRequest, DeviceSelectResponse

logger = logging.getLogger(__name__)


class TelephonyClient:
    def __init__(self, base_url: str, timeout: float = 1.0) -> None:
        self._base = base_url.rstrip("/")
        self._client = httpx.AsyncClient(timeout=timeout)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def select_device(self, campaign_id: int) -> DeviceSelectResponse | None:
        payload = DeviceSelectRequest(campaign_id=campaign_id)
        try:
            resp = await self._client.post(
                f"{self._base}/devices/select",
                json=payload.model_dump(mode="json"),
            )
        except httpx.HTTPError as exc:
            logger.warning("device_select_http_error campaign_id=%d err=%r", campaign_id, exc)
            return None

        if resp.status_code != 200:
            logger.warning(
                "device_select_non_200 campaign_id=%d status=%d body=%s",
                campaign_id,
                resp.status_code,
                resp.text[:200],
            )
            return None

        try:
            return DeviceSelectResponse.model_validate_json(resp.text)
        except Exception as exc:
            logger.warning("device_select_decode_error campaign_id=%d err=%r", campaign_id, exc)
            return None
