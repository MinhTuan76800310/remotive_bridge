"""Tests for the health server.

The contract worth defending: `/health` returns **200 while the process is
alive**, and degradation lives in the body. A bridge waiting for a peer is
working as designed — an orchestrator that restarts it on a 503 would break the
one behaviour the retry loops exist to provide.
"""

from __future__ import annotations

import pytest
from aiohttp.test_utils import TestClient, TestServer

from kx_vss_bridge.health import create_health_app
from kx_vss_bridge.state import BridgeState, Direction, Peer


async def _client(state: BridgeState) -> TestClient:
    client = TestClient(TestServer(create_health_app(state)))
    await client.start_server()
    return client


async def _connected() -> BridgeState:
    state = BridgeState()
    await state.set_peer(Peer.REMOTIVE, connected=True, phase="streaming")
    await state.set_peer(Peer.KUKSA, connected=True, phase="writing")
    return state


async def test_health_is_200_while_starting_up():
    client = await _client(BridgeState())
    try:
        assert (await client.get("/health")).status == 200
    finally:
        await client.close()


async def test_health_is_200_even_when_a_peer_is_down():
    """The bridge is retrying, which is correct behaviour, not a failure."""
    state = BridgeState()
    await state.set_peer(Peer.REMOTIVE, connected=True, phase="streaming")
    client = await _client(state)
    try:
        response = await client.get("/health")
        assert response.status == 200
        assert (await response.json())["status"] == "degraded"
    finally:
        await client.close()


async def test_status_is_ok_only_when_both_peers_are_up():
    client = await _client(await _connected())
    try:
        assert (await (await client.get("/health")).json())["status"] == "ok"
    finally:
        await client.close()


async def test_a_skipped_mapping_keeps_status_degraded():
    state = await _connected()
    await state.replace_validation(
        skipped=[{"entry": "F.S", "reason": "not in vehicle"}], warnings=[]
    )
    client = await _client(state)
    try:
        assert (await (await client.get("/health")).json())["status"] == "degraded"
    finally:
        await client.close()


async def test_health_is_compact():
    """Small enough to read at a glance; /stats carries the detail."""
    client = await _client(await _connected())
    try:
        body = await (await client.get("/health")).json()
        assert set(body) == {"status", "uptime_s", "remotive", "kuksa", "mapping"}
        assert "drops" not in body
    finally:
        await client.close()


async def test_stats_carries_the_full_snapshot():
    state = await _connected()
    await state.record_drop(Direction.TO_VSS, "F.S", "outside range")
    await state.replace_validation(
        skipped=[], warnings=[{"frame": "F", "note": "two transmitters"}]
    )
    client = await _client(state)
    try:
        body = await (await client.get("/stats")).json()
        assert body["drops"][0]["entry"] == "F.S"
        assert body["warnings"][0]["note"] == "two transmitters"
        assert body["mapping"]["to_vss_drops"] == 1
    finally:
        await client.close()


async def test_responses_are_json_and_uncached():
    client = await _client(await _connected())
    try:
        response = await client.get("/stats")
        assert response.content_type == "application/json"
        assert response.headers["Cache-Control"] == "no-store"
    finally:
        await client.close()


async def test_an_unknown_path_is_404():
    client = await _client(BridgeState())
    try:
        assert (await client.get("/nope")).status == 404
    finally:
        await client.close()


async def test_no_secret_or_signal_value_is_exposed():
    """/stats is diagnostics. Buffered payloads and tokens stay internal."""
    state = await _connected()
    await state.put_latest(Direction.TO_VSS, "Vehicle.Speed", 88.8)
    client = await _client(state)
    try:
        text = await (await client.get("/stats")).text()
        assert "88.8" not in text
        assert "token" not in text.lower()
    finally:
        await client.close()
