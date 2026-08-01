"""Tests for the entrypoint.

Two things matter here and nothing else: a bad config must fail *now*, loudly,
with a non-zero exit; and once running, nothing short of cancellation may take
the process down — a peer outage is a degraded state, not a reason to exit.
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import re
import sys

import pytest
import structlog

from kx_vss_bridge.__main__ import _configure_logging, build_parser, run

_DIM_ESC = "\x1b[2m"
_RESET_ESC = "\x1b[0m"

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


# ── console logging ──────────────────────────────────────────────────────────


def _emit(log_format, is_tty, emit, *, root_level=None):
    """Configure logging, capture one emission, always restore global state.

    Traps, all four hit while validating this task:

    * **pytest installs its own root handlers** (`_LiveLoggingNullHandler`,
      `_FileHandler`), and `logging.basicConfig()` is a no-op when the root
      logger already has handlers. Without clearing them first,
      `_configure_logging` installs nothing, the third-party line goes to
      pytest's capture instead of the buffer, and the dimming tests fail
      against an empty string. Clearing and restoring is what makes the test
      exercise the production path.
    * structlog's default PrintLogger writes to `sys.stdout`, while the stdlib
      handler writes to its own stream — so both are redirected. Patching
      `sys.stdout` is *sufficient* for the default factory: `PrintLogger.msg`
      passes `file=None` to `print` when its file is `stdout`, so the stream is
      resolved at call time. This helper therefore must **not** install a
      `logger_factory` of its own — doing so overwrote the factory the
      implementation chose and blinded every test below to the one design the
      task exists to forbid (see `_assert_not_routed_through_stdlib`).
    * `KX_LOG_FORMAT` is read from the real environment when `log_format` is
      None, so the ambient value is cleared — otherwise anyone with that
      variable exported gets a red suite for a reason unrelated to the tests.
    * The stdlib root level is left alone unless `root_level` asks for it, and
      it is restored either way.
    """
    buf = io.StringIO()
    root = logging.getLogger()
    saved_handlers = root.handlers[:]
    saved_level = root.level
    saved_stdout = sys.stdout
    saved_config = structlog.get_config()
    saved_env = os.environ.pop("KX_LOG_FORMAT", None)
    root.handlers = []
    try:
        _configure_logging("INFO", log_format=log_format, is_tty=is_tty)
        _assert_not_routed_through_stdlib()
        for handler in root.handlers:
            if hasattr(handler, "stream"):
                handler.stream = buf
        sys.stdout = buf
        if root_level is not None:
            root.setLevel(root_level)
        emit()
    finally:
        sys.stdout = saved_stdout
        root.handlers = saved_handlers
        root.setLevel(saved_level)
        structlog.configure(**saved_config)
        if saved_env is not None:
            os.environ["KX_LOG_FORMAT"] = saved_env
    return buf.getvalue()


def _assert_not_routed_through_stdlib():
    """The crux constraint of this task, checked on every `_emit` call.

    Routing the bridge's own events through `structlog.stdlib.LoggerFactory`
    makes the root stdlib logger a *second level gate*: with the root logger at
    WARNING and structlog's wrapper_class at INFO, an info event is silently
    dropped. That is a behaviour change, not formatting, so the renderer work
    must never introduce it.

    Asserted here rather than in one test so that every case below carries the
    guard, and paired with
    `test_an_info_event_survives_a_root_logger_at_warning`, which exercises the
    failure mode itself rather than its structural cause.
    """
    factory = structlog.get_config()["logger_factory"]
    assert not isinstance(factory, structlog.stdlib.LoggerFactory), (
        "_configure_logging installed structlog.stdlib.LoggerFactory; that makes "
        "the root logger a second level gate and silently drops events"
    )


def _bridge_event():
    structlog.get_logger("kx").warning(
        "mapping warning", direction="to_can", frame="VC_To_HMI"
    )


def _third_party_event():
    logging.getLogger("kuksa_client.grpc").info("No Root CA present")


def test_unset_format_without_a_tty_gives_json():
    parsed = json.loads(_emit(None, False, _bridge_event))
    assert parsed["event"] == "mapping warning"
    assert parsed["direction"] == "to_can"


def test_unset_format_with_a_tty_gives_console():
    line = _emit(None, True, _bridge_event)
    with pytest.raises(json.JSONDecodeError):
        json.loads(line)
    assert "mapping warning" in line


def test_explicit_console_overrides_a_missing_tty():
    line = _emit("console", False, _bridge_event)
    with pytest.raises(json.JSONDecodeError):
        json.loads(line)


def test_explicit_json_overrides_a_present_tty():
    json.loads(_emit("json", True, _bridge_event))  # must not raise


def test_an_unrecognised_format_refuses_to_start():
    with pytest.raises(ValueError, match="KX_LOG_FORMAT"):
        _configure_logging("INFO", log_format="colour", is_tty=False)


@pytest.mark.parametrize("log_format, is_tty", [("json", False), ("console", True)])
def test_an_info_event_survives_a_root_logger_at_warning(log_format, is_tty):
    """The failure mode that got the stdlib-routing design rejected.

    `_configure_logging` sets structlog's wrapper_class to INFO, so an info
    event must be emitted no matter what level the *stdlib* root logger sits
    at. Routing structlog through `structlog.stdlib.LoggerFactory` breaks
    exactly this: the root logger becomes a second gate and the event vanishes
    with no error anywhere. Verified to fail against that design, in both
    renderer modes.
    """
    out = _emit(
        log_format,
        is_tty,
        lambda: structlog.get_logger("kx").info("info survives", n=1),
        root_level=logging.WARNING,
    )
    assert "info survives" in out


def test_console_colour_appears_only_in_the_level_column():
    line = _emit("console", True, _bridge_event)
    match = re.search(r"\x1b\[3[0-7]m", line)
    assert match, "expected a colour escape in the level column"
    tail = line[match.end():]
    stray = [
        esc
        for esc in re.findall(r"\x1b\[[0-9;]*m", tail)
        if esc not in (_DIM_ESC, _RESET_ESC)
    ]
    assert stray == [], f"colour leaked outside the level column: {stray}"


def test_third_party_lines_are_dimmed_in_console_mode():
    line = _emit("console", True, _third_party_event)
    assert line.startswith(_DIM_ESC)
    assert line.rstrip("\n").endswith(_RESET_ESC)


def test_third_party_lines_are_untouched_in_json_mode():
    """No escape, and the message is exactly what kuksa_client emitted —
    JSON mode must be byte-identical to today, third-party lines included."""
    out = _emit("json", False, _third_party_event)
    assert "\x1b" not in out
    assert out.strip() == "No Root CA present"


def test_json_mode_output_shape_is_unchanged():
    """The original bug report's line shape: kwargs first, in insertion order,
    then event, level, timestamp. A renderer swap must not reorder or rename
    anything, or every existing log consumer breaks.
    """
    line = _emit(
        None,
        False,
        lambda: structlog.get_logger("kx").info(
            "starting", remotive="http://127.0.0.1:50106", to_vss=4, to_can=2
        ),
    )
    assert list(json.loads(line)) == [
        "remotive",
        "to_vss",
        "to_can",
        "event",
        "level",
        "timestamp",
    ]
