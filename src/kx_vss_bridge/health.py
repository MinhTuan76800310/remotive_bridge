"""The `/health` and `/stats` endpoints.

`docker logs` cannot answer "is it working *now*". The bridge deliberately never
exits, so process liveness carries no information either — a container that is up
may be happily streaming or stuck retrying a broker that will never return.

Hence one rule, and it is the whole design of this module: **`/health` returns 200
whenever the process can answer.** Degradation is reported in the body, not in the
status code. A bridge waiting for KUKSA is working exactly as designed, and an
orchestrator that restarted it on a 503 would destroy the retry behaviour the two
loops exist to provide.

`/health` is the compact view; `/stats` is everything, including per-entry drop
reasons and validation warnings.
"""

from __future__ import annotations

import asyncio
from typing import Any

from aiohttp import web

from kx_vss_bridge.state import BridgeState

__all__ = ["create_health_app", "serve_health"]

_HEALTH_KEYS = ("status", "uptime_s", "remotive", "kuksa", "mapping")


def _json(payload: dict[str, Any]) -> web.Response:
    # no-store because every field is a live reading; a cached /health is a lie.
    return web.json_response(payload, headers={"Cache-Control": "no-store"})


def create_health_app(state: BridgeState) -> web.Application:
    async def health(_: web.Request) -> web.Response:
        snapshot = await state.snapshot()
        return _json({key: snapshot[key] for key in _HEALTH_KEYS})

    async def stats(_: web.Request) -> web.Response:
        return _json(await state.snapshot())

    app = web.Application()
    app.add_routes([web.get("/health", health), web.get("/stats", stats)])
    return app


async def serve_health(state: BridgeState, host: str, port: int) -> None:
    """Serve until cancelled."""
    runner = web.AppRunner(create_health_app(state), access_log=None)
    await runner.setup()
    try:
        await web.TCPSite(runner, host, port).start()
        # start() returns as soon as the socket is listening; the server runs in
        # background tasks. Park here so the supervisor's cancellation lands on
        # this task and reaches the cleanup below.
        await asyncio.Event().wait()
    finally:
        await runner.cleanup()
