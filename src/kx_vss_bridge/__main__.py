"""Entrypoint: parse the mapping, start three tasks, stay up.

    kx-vss-bridge --config /config/mapping.yaml

Two rules, and the whole module is about the boundary between them.

**Before the loops start, be strict.** An unreadable file, a broken peer section
or a mapping where nothing survives is a configuration error the operator must
fix, and starting anyway would only hide it behind a healthy-looking container.
Those exit non-zero.

**Once running, never exit.** A peer being down is the normal case the retry
loops exist for. The container's absence would be a worse signal than a
`/health` body saying `degraded`, and there is nothing useful to hand over to.
Malformed *entries* follow the same logic: they are dropped, recorded in
`/stats`, and the remaining mappings run.

The direction supervisors own peer reconnection. The task group here only guards
the unexpected — a worker that returns or raises when it should not — and
restarts that one task.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import signal
import sys
from pathlib import Path
from typing import Any, Awaitable, Callable

import structlog
from kuksa_client.grpc.aio import VSSClient
from remotivelabs.broker import BrokerClient
from structlog.dev import (
    Column,
    ConsoleRenderer,
    KeyValueColumnFormatter,
    LogLevelColumnFormatter,
)

from kx_vss_bridge.config import BridgeConfig, ConfigError, load_config
from kx_vss_bridge.health import serve_health
from kx_vss_bridge.remotive_to_vss import run_remotive_to_vss
from kx_vss_bridge.state import BridgeState
from kx_vss_bridge.vss_to_remotive import run_vss_to_remotive

__all__ = ["build_parser", "main", "run"]

log = structlog.get_logger(__name__)

DEFAULT_CONFIG = Path("/config/mapping.yaml")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="kx-vss-bridge",
        description="Bridge RemotiveLabs CAN signals and KUKSA VSS signals, both ways.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help=f"mapping file (default: {DEFAULT_CONFIG})",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="log level (default: INFO)",
    )
    return parser


_DIM = "\x1b[2m"
_RESET = "\x1b[0m"


def _console_columns() -> list[Column]:
    """A-lite: colour lives only in the level column.

    ConsoleRenderer's defaults colour cyan keys, magenta values and a bold
    event name. Measured on one warning line: 23 ANSI escapes, 151 characters
    of content rendered as 252 — and the yellow level marker then competes
    with everything around it. Restricting colour to the level keeps the one
    cue that matters legible.
    """
    return [
        Column(
            "timestamp",
            KeyValueColumnFormatter(
                key_style=None, value_style=_DIM, reset_style=_RESET, value_repr=str
            ),
        ),
        Column(
            "level",
            LogLevelColumnFormatter(
                level_styles=ConsoleRenderer.get_default_level_styles(),
                reset_style=_RESET,
            ),
        ),
        Column(
            "event",
            KeyValueColumnFormatter(
                key_style=None,
                value_style="",
                reset_style="",
                value_repr=str,
                width=24,
            ),
        ),
        Column(
            "",
            KeyValueColumnFormatter(
                key_style=_DIM, value_style="", reset_style=_RESET, value_repr=str
            ),
        ),
    ]


class _DimFormatter(logging.Formatter):
    """Dim a third-party stdlib log line, whole.

    `kuksa_client` logs `No Root CA present` and `Establishing insecure
    channel` through stdlib logging, so they bypass structlog entirely and
    print raw in the middle of our output. Dimming them here — at the stdlib
    handler — leaves structlog's own path untouched, which is what keeps this
    change presentational.

    Installed in console mode only; in JSON mode the handler keeps its
    original formatter so output stays byte-identical to today's.
    """

    def format(self, record: logging.LogRecord) -> str:
        return _DIM + super().format(record) + _RESET


def _configure_logging(
    level: str,
    *,
    log_format: str | None = None,
    is_tty: bool | None = None,
) -> None:
    """Configure logging. Presentation only — the processor chain is today's.

    `log_format` and `is_tty` exist for tests; production passes neither and
    the defaults read KX_LOG_FORMAT and sys.stdout.isatty().
    """
    chosen = log_format if log_format is not None else os.environ.get("KX_LOG_FORMAT")
    if chosen not in (None, "console", "json"):
        # A typo must not look like it worked.
        raise ValueError(f"KX_LOG_FORMAT must be 'console' or 'json', got {chosen!r}")
    if chosen is None:
        console = sys.stdout.isatty() if is_tty is None else is_tty
    else:
        console = chosen == "console"

    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=level)

    renderer = (
        ConsoleRenderer(colors=True, columns=_console_columns())
        if console
        else structlog.processors.JSONRenderer()
    )
    # Identical to the previous configuration apart from `renderer`: same
    # processors, same wrapper_class, same default logger factory. Routing
    # these events through stdlib instead would make the root logger's level
    # a second gate — measured to silently drop an info event when the root
    # logger sits at WARNING — which is a behaviour change, not formatting.
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelName(level)
        ),
    )

    if console:
        for handler in logging.getLogger().handlers:
            handler.setFormatter(_DimFormatter("%(message)s"))


def _token(config: BridgeConfig) -> str | None:
    """Read the KUKSA token at client-creation time, never at parse time.

    Keeping it out of the config object is what guarantees it cannot reach a
    state snapshot or a log line.
    """
    if config.kuksa.token_path is None:
        return None
    return config.kuksa.token_path.read_text().strip()


async def _supervise(
    name: str,
    factory: Callable[[], Awaitable[None]],
    retry_delay: float,
) -> None:
    """Keep one task alive. Cancellation passes through; nothing else does."""
    while True:
        try:
            await factory()
            log.error("worker returned unexpectedly; restarting", worker=name)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.error("worker crashed; restarting", worker=name, error=str(exc))
        await asyncio.sleep(retry_delay)


async def run(
    config_path: Path,
    *,
    to_vss: Callable[..., Awaitable[None]] = run_remotive_to_vss,
    to_can: Callable[..., Awaitable[None]] = run_vss_to_remotive,
    health: Callable[..., Awaitable[None]] = serve_health,
) -> None:
    """Load the mapping and run until cancelled.

    The three workers are injectable so the lifecycle can be tested without a
    broker, a databroker, or a socket.
    """
    try:
        loaded = load_config(config_path)
    except ConfigError as exc:
        log.error("cannot start", error=str(exc), config=str(config_path))
        raise SystemExit(2) from exc

    config = loaded.config
    state = BridgeState()

    # Seed /stats before anything starts, so a dropped entry is visible from the
    # very first request rather than appearing once a peer happens to connect.
    await state.replace_validation(
        skipped=[
            {"entry": item.entry, "reason": item.reason, "section": item.section}
            for item in loaded.skipped
        ],
        warnings=[],
        to_vss=len(config.to_vss),
        to_can=len(config.to_can),
    )
    for item in loaded.skipped:
        log.error("mapping entry dropped", entry=item.entry, reason=item.reason)

    log.info(
        "starting",
        remotive=config.remotive.url,
        kuksa=f"{config.kuksa.host}:{config.kuksa.port}",
        to_vss=len(config.to_vss),
        to_can=len(config.to_can),
        health=f"{config.options.health_host}:{config.options.health_port}",
    )

    token = _token(config)

    def broker_factory() -> Any:
        return BrokerClient(url=config.remotive.url, client_id="kx-vss-bridge")

    def kuksa_factory() -> Any:
        return VSSClient(config.kuksa.host, config.kuksa.port, token=token)

    peers = {"broker_factory": broker_factory, "kuksa_factory": kuksa_factory}
    retry = config.options.retry_delay

    async with asyncio.TaskGroup() as group:
        group.create_task(
            _supervise("remotive-to-vss", lambda: to_vss(config, state, **peers), retry),
            name="remotive-to-vss",
        )
        group.create_task(
            _supervise("vss-to-remotive", lambda: to_can(config, state, **peers), retry),
            name="vss-to-remotive",
        )
        group.create_task(
            _supervise(
                "health",
                lambda: health(
                    state, config.options.health_host, config.options.health_port
                ),
                retry,
            ),
            name="health",
        )


async def _run_until_signalled(config_path: Path) -> None:
    """Run the bridge, shutting down cleanly on SIGTERM or SIGINT.

    Without this, Python's default SIGTERM handler kills the process outright:
    no task cancellation, no `runner.cleanup()`, so the health socket and both
    gRPC connections are dropped rather than closed. `docker stop` sends SIGTERM,
    so that is the normal shutdown path, not an edge case.
    """
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()

    for signame in ("SIGTERM", "SIGINT"):
        try:
            loop.add_signal_handler(getattr(signal, signame), stop.set)
        except (NotImplementedError, AttributeError):  # pragma: no cover
            pass  # not POSIX; fall back to whatever the platform does

    worker = asyncio.create_task(run(config_path), name="bridge")
    stopped = asyncio.create_task(stop.wait(), name="signal")

    try:
        done, pending = await asyncio.wait(
            {worker, stopped}, return_when=asyncio.FIRST_COMPLETED
        )

        if worker in done:
            # run() never returns normally, so this is an error or SystemExit.
            await worker
            return

        log.info("signal received; shutting down")
        worker.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker
    finally:
        # Cancelling is not enough: a cancelled task stays pending until it is
        # awaited, and the event loop will not close while it is. Missing this
        # left the process alive after a clean shutdown — SIGTERM logged, restbus
        # stopped, and then nothing.
        stopped.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await stopped
        # Remove the handlers so a second signal reaches the default disposition
        # rather than setting an Event nobody is waiting on.
        for signame in ("SIGTERM", "SIGINT"):
            with contextlib.suppress(NotImplementedError, AttributeError, ValueError):
                loop.remove_signal_handler(getattr(signal, signame))


def main() -> None:
    args = build_parser().parse_args()
    _configure_logging(args.log_level)
    try:
        asyncio.run(_run_until_signalled(args.config))
    except KeyboardInterrupt:  # pragma: no cover - racing the handler above
        log.info("interrupted; shutting down")


if __name__ == "__main__":
    main()
