"""Tests for the entrypoint.

Two things matter here and nothing else: a bad config must fail *now*, loudly,
with a non-zero exit; and once running, nothing short of cancellation may take
the process down — a peer outage is a degraded state, not a reason to exit.
"""

from __future__ import annotations

import asyncio

import pytest

from kx_vss_bridge.__main__ import build_parser, run

VALID = """
remotive: {url: http://broker:50051}
kuksa: {host: kuksa, port: 55557}
options: {seed_seconds: 0.05, retry_delay: 0.05, health_port: 18090}
to_vss:
  - can: {namespace: NS, signal: F.S}
    vss: Vehicle.Speed
    type: float
"""


def _write(tmp_path, text: str = VALID):
    path = tmp_path / "mapping.yaml"
    path.write_text(text)
    return path


# ── the CLI ──────────────────────────────────────────────────────────────────


def test_config_defaults_to_the_container_mount_point():
    assert build_parser().parse_args([]).config.as_posix() == "/config/mapping.yaml"


def test_config_can_be_overridden():
    args = build_parser().parse_args(["--config", "/tmp/other.yaml"])
    assert args.config.as_posix() == "/tmp/other.yaml"


# ── startup failures ─────────────────────────────────────────────────────────


async def test_a_missing_config_exits_non_zero(tmp_path):
    with pytest.raises(SystemExit) as exc:
        await run(tmp_path / "absent.yaml")
    assert exc.value.code != 0


async def test_an_invalid_config_exits_non_zero(tmp_path):
    with pytest.raises(SystemExit) as exc:
        await run(_write(tmp_path, "remotive: [unclosed"))
    assert exc.value.code != 0


async def test_a_config_with_no_usable_mapping_exits_non_zero(tmp_path):
    text = VALID.replace("signal: F.S", "signal: BareName")
    with pytest.raises(SystemExit) as exc:
        await run(_write(tmp_path, text))
    assert exc.value.code != 0


# ── running ──────────────────────────────────────────────────────────────────


async def test_both_directions_and_the_health_server_start(tmp_path):
    started: set[str] = set()

    async def fake(name):
        async def worker(*args, **kwargs):
            started.add(name)
            await asyncio.Event().wait()

        return worker

    task = asyncio.create_task(
        run(
            _write(tmp_path),
            to_vss=await fake("to_vss"),
            to_can=await fake("to_can"),
            health=await fake("health"),
        )
    )
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert started == {"to_vss", "to_can", "health"}


async def test_malformed_entries_are_surfaced_before_the_loops_start(tmp_path):
    """A dropped mapping must be visible on /stats from the first request."""
    seen: dict[str, object] = {}

    async def capture(config, state, **kwargs):
        seen["skipped"] = (await state.snapshot())["mapping"]["skipped"]
        await asyncio.Event().wait()

    async def idle(*args, **kwargs):
        await asyncio.Event().wait()

    text = VALID + """  - can: {namespace: NS, signal: F.T}
    vss: Vehicle.Other
"""
    task = asyncio.create_task(
        run(_write(tmp_path, text), to_vss=capture, to_can=idle, health=idle)
    )
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert seen["skipped"]
    assert "type" in seen["skipped"][0]["reason"]


async def test_a_crashing_worker_is_restarted_not_fatal(tmp_path):
    """The loops retry internally; this guards the unexpected case."""
    attempts = {"n": 0}

    async def flaky(*args, **kwargs):
        attempts["n"] += 1
        raise RuntimeError("unexpected")

    async def idle(*args, **kwargs):
        await asyncio.Event().wait()

    task = asyncio.create_task(
        run(_write(tmp_path), to_vss=flaky, to_can=idle, health=idle)
    )
    await asyncio.sleep(0.3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert attempts["n"] > 1  # restarted rather than killing the process


async def test_cancellation_shuts_everything_down(tmp_path):
    stopped: set[str] = set()

    def worker(name):
        async def run_worker(*args, **kwargs):
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                stopped.add(name)
                raise

        return run_worker

    task = asyncio.create_task(
        run(
            _write(tmp_path),
            to_vss=worker("to_vss"),
            to_can=worker("to_can"),
            health=worker("health"),
        )
    )
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert stopped == {"to_vss", "to_can", "health"}
