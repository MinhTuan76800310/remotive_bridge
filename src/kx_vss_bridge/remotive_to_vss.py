"""Loop A — Remotive to VSS.

CAN signals become VSS **current values**: what the vehicle observes and reports.

Two independent workers, joined by a bounded latest-value buffer:

    run_remotive_reader        owns the broker connection
        │  put_latest()
        ▼
    [ one slot per VSS path ]
        │  pending_snapshot() / acknowledge()
        ▼
    run_kuksa_current_writer   owns the KUKSA connection

Splitting them is what makes a KUKSA outage survivable. A single connection
scope wrapping both peers would tear down a perfectly healthy broker stream every
time the databroker hiccuped, and lose the seed phase with it. Here the reader
keeps consuming CAN into the buffer, newer values replace older ones per path,
and the writer flushes the whole snapshot when it reconnects.

The reader has two phases per connection:

* **seed** — `on_change=False`, briefly. `on_change=True` delivers nothing until
  a value moves, so a signal that changes hourly would be absent from VSS for an
  hour after every reconnect. Seeding costs `seed_seconds` and removes that.
* **stream** — `on_change=True`. The restbus already handles cyclic
  transmission; re-sending an unchanged value would be pure waste.

`read_signals()` would be the obvious way to seed, but it does not exist on
BrokerClient 0.9.1 despite `app/signals/service.py:204` calling it. `initial_empty`
is not a snapshot either — it consumes a sync marker and discards it. Opening
briefly with `on_change=False` uses only verified API.
"""

from __future__ import annotations

import asyncio
from typing import Any, Callable

import structlog
from kuksa_client.grpc import DataEntry, Datapoint, DataType, EntryUpdate, Field, Metadata

from kx_vss_bridge.config import BridgeConfig, ValueType
from kx_vss_bridge.state import BridgeState, Direction, Peer
from kx_vss_bridge.transform import TransformError, can_to_vss
from kx_vss_bridge.validation import validate_mapping

__all__ = [
    "run_kuksa_current_writer",
    "run_remotive_reader",
    "run_remotive_to_vss",
]

log = structlog.get_logger(__name__)

KUKSA_TYPES = {
    ValueType.BOOLEAN: DataType.BOOLEAN,
    ValueType.STRING: DataType.STRING,
    ValueType.INT: DataType.INT64,
    ValueType.FLOAT: DataType.FLOAT,
}


async def _drain_batches(stream: Any, handle: Callable[[list[Any]], Any]) -> None:
    async for batch in stream:
        await handle(batch)


async def run_remotive_reader(
    config: BridgeConfig,
    state: BridgeState,
    *,
    broker_factory: Callable[[], Any],
    kuksa_factory: Callable[[], Any],
    sleep: Callable[[float], Any] = asyncio.sleep,
) -> None:
    """Consume CAN into the hand-off buffer. Never returns.

    KUKSA is touched only for validation — checking the mapped paths exist —
    which is why `kuksa_factory` appears in a function that writes nothing.
    """
    retry_delay = config.options.retry_delay

    while True:
        try:
            async with broker_factory() as broker:
                # Re-validate per connection: a rebuilt vehicle can have
                # different signals, and cached results would quietly lie.
                async with kuksa_factory() as kuksa:
                    validated = await validate_mapping(config, broker, kuksa)

                await state.replace_validation(
                    skipped=validated.skipped,
                    warnings=validated.warnings,
                    to_vss=validated.active_to_vss,
                    to_can=validated.active_to_can,
                )

                if not validated.subscription_groups:
                    log.warning("no to_vss mappings survived validation; idling")
                    await state.set_peer(Peer.REMOTIVE, connected=True, phase="idle")
                    await sleep(retry_delay)
                    continue

                index = {
                    (m.can.namespace, m.can.signal): m for m in validated.to_vss
                }
                groups = tuple(validated.subscription_groups)

                async def handle(batch: list[Any]) -> None:
                    await state.record_can_batch(len(batch))
                    for sig in batch:
                        mapping = index.get((sig.namespace, sig.name))
                        if mapping is None:
                            continue  # subscribed by frame; not every signal is mapped
                        try:
                            value = can_to_vss(mapping, sig.value)
                        except TransformError as exc:
                            await state.record_drop(
                                Direction.TO_VSS, mapping.can.signal, str(exc)
                            )
                            continue
                        await state.put_latest(Direction.TO_VSS, mapping.vss, value)

                # ── seed ──────────────────────────────────────────────────────
                await state.set_peer(Peer.REMOTIVE, connected=True, phase="seeding")
                seed_stream = await broker.subscribe(*groups, on_change=False)
                try:
                    # The timeout must interrupt the iteration, not merely be
                    # checked between batches: a quiet broker yields nothing and
                    # `async for` would block past the deadline forever.
                    async with asyncio.timeout(config.options.seed_seconds):
                        await _drain_batches(seed_stream, handle)
                except (asyncio.TimeoutError, TimeoutError):
                    pass  # the expected way to leave the seed phase

                # ── stream ────────────────────────────────────────────────────
                await state.set_peer(Peer.REMOTIVE, connected=True, phase="streaming")
                stream = await broker.subscribe(*groups, on_change=True)
                await _drain_batches(stream, handle)

                # A stream that ends without raising is not an error, but it is
                # not success either — fall through to the shared backoff.
                log.warning("remotive stream ended; reconnecting")
                await state.set_peer(Peer.REMOTIVE, connected=False, phase="reconnecting")

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("remotive reader error", error=str(exc))
            await state.record_reconnect(Peer.REMOTIVE, exc)

        # Outside the except: a clean stream end must back off too, or the loop
        # spins reconnecting as fast as the broker will accept.
        await sleep(retry_delay)


async def run_kuksa_current_writer(
    config: BridgeConfig,
    state: BridgeState,
    *,
    kuksa_factory: Callable[[], Any],
    sleep: Callable[[float], Any] = asyncio.sleep,
) -> None:
    """Flush the hand-off buffer into KUKSA current values. Never returns."""
    retry_delay = config.options.retry_delay
    types = {m.vss: KUKSA_TYPES[m.value_type] for m in config.to_vss}

    while True:
        try:
            async with kuksa_factory() as kuksa:
                await state.set_peer(Peer.KUKSA, connected=True, phase="writing")

                while True:
                    # On a fresh connection there may already be a backlog, so
                    # take a snapshot before waiting for the next change.
                    version, pending = await state.pending_snapshot(Direction.TO_VSS)
                    if not pending:
                        await state.wait_for_pending(Direction.TO_VSS)
                        continue

                    updates = [
                        EntryUpdate(
                            DataEntry(
                                path=path,
                                value=Datapoint(value),
                                # Declaring the type avoids a get_value_types()
                                # round-trip before every single write — the
                                # reason `type` is mandatory in the mapping.
                                metadata=Metadata(
                                    data_type=types.get(path, DataType.UNSPECIFIED)
                                ),
                            ),
                            (Field.VALUE,),
                        )
                        for path, value in pending.items()
                    ]

                    await kuksa.set(updates, try_v2=True)
                    # Version-scoped: anything the reader stored while this write
                    # was in flight survives and goes out next round.
                    await state.acknowledge(Direction.TO_VSS, version)
                    await state.record_write(Direction.TO_VSS, len(updates))

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("kuksa writer error", error=str(exc))
            await state.record_reconnect(Peer.KUKSA, exc)

        await sleep(retry_delay)


async def run_remotive_to_vss(
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
            run_remotive_reader(
                config, state,
                broker_factory=broker_factory,
                kuksa_factory=kuksa_factory,
                sleep=sleep,
            ),
            name="remotive-reader",
        )
        group.create_task(
            run_kuksa_current_writer(
                config, state, kuksa_factory=kuksa_factory, sleep=sleep
            ),
            name="kuksa-current-writer",
        )
