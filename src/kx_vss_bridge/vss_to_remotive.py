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

Targets are seeded with `get_target_values()` before subscribing, because
`subscribe_target_values` reports *changes*. A target set while the bridge was
down would otherwise never arrive.
"""

from __future__ import annotations

import asyncio
from typing import Any, Callable

import structlog
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

                # Seed first: subscribe_target_values only reports changes, so a
                # target set before we connected would never be delivered.
                for path, datapoint in (await kuksa.get_target_values(paths)).items():
                    mapping = index.get(path)
                    if mapping is not None:
                        await _buffer_target(state, mapping, datapoint)

                async for updates in kuksa.subscribe_target_values(paths):
                    for path, datapoint in updates.items():
                        mapping = index.get(path)
                        if mapping is None:
                            continue
                        await _buffer_target(state, mapping, datapoint)

                log.warning("kuksa target stream ended; reconnecting")
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

    while True:
        try:
            async with broker_factory() as broker:
                async with kuksa_factory() as kuksa:
                    validated = await validate_mapping(config, broker, kuksa)

                if not validated.to_can:
                    log.warning("no to_can mappings survived validation; idling")
                    await sleep(retry_delay)
                    continue

                # One call, every frame, once per connection. Restbus state dies
                # with the connection, so this repeats on every reconnect.
                add_args = tuple(
                    (namespace, list(frames.values()))
                    for namespace, frames in validated.restbus_frames.items()
                )
                if add_args:
                    await broker.restbus.add(*add_args, start=True)

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
