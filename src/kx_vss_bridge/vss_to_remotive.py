"""Loop B — VSS to Remotive.

VSS **actuation targets** become CAN values: what a function commands, not what
the vehicle observes. The bridge writes the value an ECU will read; whether the
ECU acts is the ECU's business. That is the same separation CPD makes between a
target and a current value, and it is why this direction reads targets while
Loop A writes current values.

Two independent workers around the same bounded buffer as Loop A:

    run_kuksa_target_reader        owns the KUKSA connection
        │  put_latest()
        ▼
    [ one slot per VSS path ]
        │  pending_snapshot() / acknowledge()
        ▼
    run_remotive_restbus_writer    owns the broker connection

Three behaviours here are settled by measurement against a live vCar on
2026-08-01, not by reading the proto (`docs/spike-f1-f6-findings.md`):

* **Every frame goes in one `add()` call.** `Add` removes the calling client's
  previous configuration, so adding frames one at a time would erase the earlier
  ones. It does *not* touch another client's frames — F1, refuted — so adding to
  a namespace an ECU owns is safe.
* **`update_signals` on a namespace with no restbus is silently ignored.** No
  error, nothing delivered (F10). Hence `add()` first, and a validation warning
  when `allow_add: false` leaves a frame undriven.
* **Writing a frame an ECU also transmits works**, but makes the bridge a second
  transmitter and the value alternates at cycle rate (F9). Validation warns; the
  bridge does not refuse, because refusing would be wrong.

Targets are read over **both KUKSA protocols at once**, v1 and v2. A bridge
carries what arrives; which protocol a writer speaks is the writer's business.
Measured on cpd-standalone-databroker 0.7.1-dev.0 (2026-08-02): a v1 target
write reaches a v1 subscriber and is invisible to a v2 subscriber, and the
client's own v1 fallback does not help because it triggers only on
UNIMPLEMENTED — which a broker that serves v2 never returns. See
`_subscribe_both_protocols`.

Targets are seeded with `get_target_values()` before subscribing, because the v2
subscription reports *changes only*: a target set while the bridge was down would
otherwise never arrive. The v1 subscription does not need the seed — measured
2026-08-02, it replays the stored target at subscribe time — but the seed is cheap
and is the only cover for v2 when the v1 branch is dormant.
"""

from __future__ import annotations

import asyncio
from typing import Any, Callable

import grpc
import structlog
from kuksa_client.grpc import Field, SubscribeEntry, View
from remotivelabs.broker.restbus import RestbusSignalConfig

from kx_vss_bridge.config import BridgeConfig
from kx_vss_bridge.state import BridgeState, Direction, Peer
from kx_vss_bridge.transform import TransformError, vss_to_can
from kx_vss_bridge.validation import validate_mapping

__all__ = [
    "run_kuksa_target_reader",
    "run_remotive_restbus_writer",
    "run_vss_to_remotive",
]

log = structlog.get_logger(__name__)


async def _buffer_target(
    state: BridgeState, mapping: Any, datapoint: Any
) -> None:
    """Invert one target and put it in the buffer, or count why we could not."""
    # An actuator with no target yet arrives as None, or as a Datapoint whose
    # value is None. Neither is a command.
    value = getattr(datapoint, "value", datapoint)
    if value is None:
        return
    try:
        converted = vss_to_can(mapping, value)
    except TransformError as exc:
        await state.record_drop(Direction.TO_CAN, mapping.vss, str(exc))
        return
    await state.put_latest(Direction.TO_CAN, mapping.vss, converted)


_UNIMPLEMENTED = grpc.StatusCode.UNIMPLEMENTED.value[0]


def _is_unimplemented(exc: BaseException) -> bool:
    """True when the databroker has no such service, as opposed to a real fault.

    `VSSClientError.error["code"]` carries the numeric gRPC code (12), set by
    `from_grpc_error`. A bare `RpcError` can also surface when the failure
    happens outside the client's own wrapping, so both shapes are checked.
    """
    error = getattr(exc, "error", None)
    if isinstance(error, dict) and error.get("code") == _UNIMPLEMENTED:
        return True
    code = getattr(exc, "code", None)
    try:
        return callable(code) and code() == grpc.StatusCode.UNIMPLEMENTED
    except Exception:  # pragma: no cover - defensive; code() should not raise
        return False


async def _drain_v2_targets(
    kuksa: Any, state: BridgeState, index: dict[str, Any], paths: tuple[str, ...]
) -> None:
    """Consume the v2 actuation stream (`OpenProviderStream`) until it ends."""
    async for updates in kuksa.subscribe_target_values(paths):
        for path, datapoint in updates.items():
            mapping = index.get(path)
            if mapping is not None:
                await _buffer_target(state, mapping, datapoint)


async def _drain_v1_targets(
    kuksa: Any, state: BridgeState, index: dict[str, Any], paths: tuple[str, ...]
) -> None:
    """Consume the v1 target stream (`Subscribe` + View.TARGET_VALUE).

    `View.TARGET_VALUE` is not the default and has to be asked for: the default
    view returns current values, which for an actuator is what an ECU reported,
    not what a function commanded.
    """
    entries = [
        SubscribeEntry(path, View.TARGET_VALUE, (Field.ACTUATOR_TARGET,))
        for path in paths
    ]
    async for updates in kuksa.subscribe(entries=entries):
        for update in updates:
            entry = getattr(update, "entry", None)
            if entry is None:
                continue
            mapping = index.get(entry.path)
            if mapping is not None:
                await _buffer_target(state, mapping, entry.actuator_target)


async def _subscribe_both_protocols(
    kuksa: Any, state: BridgeState, index: dict[str, Any], paths: tuple[str, ...]
) -> None:
    """Read targets over v1 AND v2 at once, tolerating either being absent.

    A bridge's job is to carry whatever arrives. Which KUKSA protocol a writer
    speaks is that writer's business, and the bridge has no standing to insist:
    one CPD instance or five, the difference is how many signals arrive, not how
    many protocols have to be understood.

    Both are needed because neither alone is sufficient in the field:

    * **v2 only** misses cpd-core 1.0.0, which pins kuksa-client==0.4.3 — a
      version with no v2 code whatsoever, so its `set_target_values` is a v1
      Set. kuksa-client 0.5.2's own v1 fallback does not save us: it triggers
      only on UNIMPLEMENTED, and the CPD databroker (0.7.1-dev) *does* serve
      /kuksa.val.v2.VAL/OpenProviderStream. Serving v2 is exactly what disables
      the fallback.
    * **v1 only** misses any newer provider that actuates over v2, including
      kuksa-client 0.5.2's own `set_target_values` against a v2 broker.

    A protocol the databroker does not serve raises here rather than yielding.
    That must not take the other one down — losing a working direction because
    of an unsupported one would make this change a regression — so each is
    supervised independently.

    But "unsupported" and "broken" are different, and flattening them would hide
    real faults. UNIMPLEMENTED means this databroker simply has no such service:
    ordinary, logged at info, dormant. Anything else — ALREADY_EXISTS when
    another provider owns the actuator (F4), a permission denial, a transport
    failure — is a fault the operator has to see, so it is recorded on the peer
    and surfaces in /stats as `last_error`.
    """

    async def guard(name: str, coro: Any) -> None:
        try:
            await coro
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if _is_unimplemented(exc):
                log.info(
                    "databroker does not serve this target protocol; ignoring it",
                    protocol=name,
                    error=str(exc),
                )
                return
            log.warning(
                "target subscription failed", protocol=name, error=str(exc)
            )
            await state.record_reconnect(Peer.KUKSA, exc)

    async with asyncio.TaskGroup() as group:
        group.create_task(
            guard("v2", _drain_v2_targets(kuksa, state, index, paths)),
            name="kuksa-targets-v2",
        )
        group.create_task(
            guard("v1", _drain_v1_targets(kuksa, state, index, paths)),
            name="kuksa-targets-v1",
        )


async def run_kuksa_target_reader(
    config: BridgeConfig,
    state: BridgeState,
    *,
    kuksa_factory: Callable[[], Any],
    sleep: Callable[[float], Any] = asyncio.sleep,
) -> None:
    """Consume VSS actuation targets into the hand-off buffer. Never returns."""
    retry_delay = config.options.retry_delay
    index = config.to_can_by_vss
    paths = tuple(index)

    if not paths:
        log.info("no to_can mappings; target reader idle")
        return

    while True:
        try:
            async with kuksa_factory() as kuksa:
                await state.set_peer(Peer.KUKSA, connected=True, phase="reading targets")

                # Seed first: the v2 subscription reports changes only, so a
                # target set before we connected would never be delivered. (v1
                # replays it, but v1 may be dormant on this databroker.)
                for path, datapoint in (await kuksa.get_target_values(paths)).items():
                    mapping = index.get(path)
                    if mapping is not None:
                        await _buffer_target(state, mapping, datapoint)

                await _subscribe_both_protocols(kuksa, state, index, paths)

                log.warning("kuksa target streams ended; reconnecting")
                await state.set_peer(Peer.KUKSA, connected=False, phase="reconnecting")

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # ALREADY_EXISTS lands here when another provider owns the actuator
            # (F4). It degrades this direction only; Loop A is untouched.
            log.warning("kuksa target reader error", error=str(exc))
            await state.record_reconnect(Peer.KUKSA, exc)

        await sleep(retry_delay)


async def run_remotive_restbus_writer(
    config: BridgeConfig,
    state: BridgeState,
    *,
    broker_factory: Callable[[], Any],
    kuksa_factory: Callable[[], Any],
    sleep: Callable[[float], Any] = asyncio.sleep,
) -> None:
    """Flush the hand-off buffer into the Remotive restbus. Never returns."""
    retry_delay = config.options.retry_delay
    # Namespaces this bridge has told a broker to start transmitting. Tracked so
    # shutdown can undo exactly what it did, and nothing else.
    started_namespaces: set[str] = set()

    while True:
        try:
            async with broker_factory() as broker:
                async with kuksa_factory() as kuksa:
                    validated = await validate_mapping(config, broker, kuksa)

                if not validated.to_can:
                    log.warning("no to_can mappings survived validation; idling")
                    await sleep(retry_delay)
                    continue

                # One call, every frame, once per connection. `add(start=True)`
                # makes the BROKER transmit these frames cyclically, and that
                # outlives this client — measured on the live rig, a frame added
                # here kept cycling at 10 Hz after the bridge process was killed.
                # Hence `started_namespaces` and the cleanup in `finally`.
                add_args = tuple(
                    (namespace, list(frames.values()))
                    for namespace, frames in validated.restbus_frames.items()
                )
                if add_args:
                    await broker.restbus.add(*add_args, start=True)
                    started_namespaces.update(ns for ns, _ in add_args)

                await state.set_peer(Peer.REMOTIVE, connected=True, phase="writing restbus")
                index = {m.vss: m for m in validated.to_can}

                while True:
                    version, pending = await state.pending_snapshot(Direction.TO_CAN)
                    if not pending:
                        await state.wait_for_pending(Direction.TO_CAN)
                        continue

                    groups: dict[str, list[RestbusSignalConfig]] = {}
                    for path, value in pending.items():
                        mapping = index.get(path)
                        if mapping is None:
                            continue  # dropped by validation since it was buffered
                        groups.setdefault(mapping.can.namespace, []).append(
                            RestbusSignalConfig.set(mapping.can.signal, value)
                        )

                    if groups:
                        await broker.restbus.update_signals(*groups.items())
                        await state.record_write(
                            Direction.TO_CAN, sum(len(g) for g in groups.values())
                        )
                    # Acknowledge regardless: an unmapped path would otherwise
                    # stay pending forever and spin this loop.
                    await state.acknowledge(Direction.TO_CAN, version)

        except asyncio.CancelledError:
            # Shutting down. Stop the frames this bridge started, so its effect
            # on the bus ends when it does. Best-effort: the broker may already
            # be gone, and failing to clean up must not mask the cancellation.
            if started_namespaces:
                try:
                    async with broker_factory() as broker:
                        await broker.restbus.close(*sorted(started_namespaces))
                    log.info(
                        "stopped restbus frames on shutdown",
                        namespaces=sorted(started_namespaces),
                    )
                except Exception as exc:
                    log.warning(
                        "could not stop restbus frames; they may keep cycling",
                        namespaces=sorted(started_namespaces),
                        error=str(exc),
                    )
            raise
        except Exception as exc:
            log.warning("remotive restbus writer error", error=str(exc))
            await state.record_reconnect(Peer.REMOTIVE, exc)

        await sleep(retry_delay)


async def run_vss_to_remotive(
    config: BridgeConfig,
    state: BridgeState,
    *,
    broker_factory: Callable[[], Any],
    kuksa_factory: Callable[[], Any],
    sleep: Callable[[float], Any] = asyncio.sleep,
) -> None:
    """Run both workers concurrently until cancelled."""
    async with asyncio.TaskGroup() as group:
        group.create_task(
            run_kuksa_target_reader(
                config, state, kuksa_factory=kuksa_factory, sleep=sleep
            ),
            name="kuksa-target-reader",
        )
        group.create_task(
            run_remotive_restbus_writer(
                config, state,
                broker_factory=broker_factory,
                kuksa_factory=kuksa_factory,
                sleep=sleep,
            ),
            name="remotive-restbus-writer",
        )
