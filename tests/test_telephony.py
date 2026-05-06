"""Tests for TelephonyClient — uses httpx.MockTransport (no network)."""

from __future__ import annotations

import httpx
import pytest

from isales_scheduler.telephony import TelephonyClient


def _make_client(handler) -> TelephonyClient:  # type: ignore[no-untyped-def]
    transport = httpx.MockTransport(handler)
    client = TelephonyClient("http://telephony.test", timeout=1.0)
    # Swap in MockTransport without touching the live network.
    client._client = httpx.AsyncClient(transport=transport, timeout=1.0)
    return client


@pytest.mark.asyncio(loop_scope="session")
async def test_select_device_success() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/devices/select"
        return httpx.Response(
            200, json={"device_id": 42, "phone_number": "13800001234"}
        )

    client = _make_client(handler)
    resp = await client.select_device(7)
    await client.aclose()
    assert resp is not None
    assert resp.device_id == 42
    assert resp.phone_number == "13800001234"


@pytest.mark.asyncio(loop_scope="session")
async def test_select_device_5xx_returns_none() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"error": "no_devices"})

    client = _make_client(handler)
    resp = await client.select_device(7)
    await client.aclose()
    assert resp is None


@pytest.mark.asyncio(loop_scope="session")
async def test_select_device_4xx_returns_none() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"error": "campaign_not_found"})

    client = _make_client(handler)
    resp = await client.select_device(7)
    await client.aclose()
    assert resp is None


@pytest.mark.asyncio(loop_scope="session")
async def test_select_device_network_error_returns_none() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("boom")

    client = _make_client(handler)
    resp = await client.select_device(7)
    await client.aclose()
    assert resp is None


@pytest.mark.asyncio(loop_scope="session")
async def test_select_device_decode_error_returns_none() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"not json")

    client = _make_client(handler)
    resp = await client.select_device(7)
    await client.aclose()
    assert resp is None
