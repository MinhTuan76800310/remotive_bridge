"""Fakes for the two peers.

Deliberately thin: they implement only the calls the bridge actually makes, and
they record what was asked of them so a test can assert on the *shape* of the
interaction — one subscribe, one batched write, one add() — not merely on the
final values.

Where the real SDK has an awkward edge, these reproduce it rather than smoothing
it over. `subscribe()` is `async def` returning an async generator (two-step, as
in remotivelabs-broker 0.9.1), and `update_signals` on a namespace with nothing
added succeeds silently, because that is what a live broker does (finding F10).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Iterable

from remotivelabs.broker import FrameInfo, Signal, SignalInfo


# ── builders ─────────────────────────────────────────────────────────────────


def make_signal_info(name: str, namespace: str, sender: list[str] | None = None) -> SignalInfo:
    return SignalInfo(
        name=name,
        namespace=namespace,
        receiver=[],
        sender=sender or [],
        named_values={},
        value_names={},
        min=0.0,
        max=0.0,
        factor=1.0,
    )


def make_frame_info(
    frame: str,
    namespace: str,
    signals: Iterable[str],
    *,
    sender: list[str] | None = None,
    cycle_time_millis: float = 100.0,
) -> FrameInfo:
    """A FrameInfo with message-qualified signal keys, as the live broker returns.

    Verified against the running rig: `list_frame_infos` gives keys like
    'VSS_VehicleState.Vehicle_Body_Horn_IsActive', already qualified.
    """
    return FrameInfo(
        name=frame,
        namespace=namespace,
        signals={s: make_signal_info(s, namespace, sender) for s in signals},
        sender=sender or [],
        receiver=[],
        cycle_time_millis=cycle_time_millis,
    )


# ── Remotive ─────────────────────────────────────────────────────────────────


@dataclass
class FakeRestbus:
    """Records add/update calls; never rejects an unconfigured frame.

    A live broker accepts `update_signals` for a frame that was never added and
    does nothing with it — there is no error to catch. Tests that care about
    that use `configured` to assert the bridge added what it needed to.
    """

    add_calls: list[tuple[tuple[Any, ...], bool]] = field(default_factory=list)
    update_calls: list[tuple[Any, ...]] = field(default_factory=list)
    configured: set[tuple[str, str]] = field(default_factory=set)
    closed: set[str] = field(default_factory=set)
    fail_add_with: Exception | None = None
    fail_update_with: Exception | None = None

    async def add(self, *frames: Any, start: bool = False) -> None:
        if self.fail_add_with is not None:
            raise self.fail_add_with
        self.add_calls.append((frames, start))
        for namespace, configs in frames:
            for config in configs:
                self.configured.add((namespace, config.name))

    async def close(self, *namespaces: str) -> None:
        """Stop transmitting and drop configuration for these namespaces."""
        self.closed.update(namespaces)
        self.configured = {
            entry for entry in self.configured if entry[0] not in namespaces
        }

    async def update_signals(self, *signals: Any) -> None:
        if self.fail_update_with is not None:
            raise self.fail_update_with
        self.update_calls.append(signals)


class FakeBrokerClient:
    """Stands in for BrokerClient over the calls the bridge makes."""

    def __init__(
        self,
        frame_infos: dict[str, list[FrameInfo]] | None = None,
        batches: list[list[Signal]] | None = None,
        *,
        seed_batches: list[list[Signal]] | None = None,
        connect_error: Exception | None = None,
        stream_error: Exception | None = None,
        hang_after_batches: bool = False,
    ) -> None:
        self._frame_infos = frame_infos or {}
        self._batches = batches or []
        # When set, the first subscribe (on_change=False) yields these instead.
        self._seed_batches = seed_batches
        self._connect_error = connect_error
        self._stream_error = stream_error
        self._hang_after_batches = hang_after_batches

        self.restbus = FakeRestbus()
        self.subscribe_calls: list[dict[str, Any]] = []
        self.entered = 0
        self.exited = 0
        self.disconnected = False

    async def __aenter__(self) -> FakeBrokerClient:
        if self._connect_error is not None:
            raise self._connect_error
        self.entered += 1
        return self

    async def __aexit__(self, *exc: object) -> None:
        self.exited += 1
        self.disconnected = True

    async def list_frame_infos(self, *namespaces: str) -> list[FrameInfo]:
        return [info for ns in namespaces for info in self._frame_infos.get(ns, [])]

    async def subscribe(
        self,
        *signals: tuple[str, list[str]],
        on_change: bool = False,
        initial_empty: bool = False,
    ) -> AsyncIterator[list[Signal]]:
        """Two-step, like the real 0.9.1 client: await, then iterate."""
        self.subscribe_calls.append(
            {"signals": signals, "on_change": on_change, "initial_empty": initial_empty}
        )
        seeding = not on_change and self._seed_batches is not None
        batches = list(self._seed_batches or []) if seeding else list(self._batches)
        error = None if seeding else self._stream_error
        hang = self._hang_after_batches

        async def generate() -> AsyncIterator[list[Signal]]:
            for batch in batches:
                yield batch
                await asyncio.sleep(0)
            if error is not None:
                raise error
            if hang:
                # A quiet broker: the stream stays open and delivers nothing.
                # Seeding must time out rather than block here forever.
                await asyncio.Event().wait()

        return generate()


# ── KUKSA ────────────────────────────────────────────────────────────────────


class FakeVSSClient:
    """Stands in for kuksa_client.grpc.aio.VSSClient."""

    def __init__(
        self,
        known_paths: Iterable[str] = (),
        *,
        target_values: dict[str, Any] | None = None,
        target_updates: list[dict[str, Any]] | None = None,
        v1_target_updates: list[dict[str, Any]] | None = None,
        connect_error: Exception | None = None,
        set_error: Exception | None = None,
        subscribe_error: Exception | None = None,
        v1_subscribe_error: Exception | None = None,
        metadata_batch_error: Exception | None = None,
    ) -> None:
        self._known = set(known_paths)
        self._target_values = target_values or {}
        self._target_updates = target_updates or []
        # Writers that reach the databroker over v1 (`set_target_values` in
        # kuksa-client 0.4.3, which has no v2 at all). A real databroker serving
        # both protocols delivers these ONLY to a v1 subscriber, which is the
        # whole reason the bridge subscribes twice.
        self._v1_target_updates = v1_target_updates or []
        self._connect_error = connect_error
        self._set_error = set_error
        self._subscribe_error = subscribe_error
        self._v1_subscribe_error = v1_subscribe_error
        # Simulates a batch get_metadata failing because one path is unknown,
        # forcing the per-path fallback.
        self._metadata_batch_error = metadata_batch_error

        self.set_calls: list[list[Any]] = []
        self.metadata_calls: list[list[str]] = []
        self.subscribe_entries: list[list[Any]] = []
        self.entered = 0

    async def __aenter__(self) -> FakeVSSClient:
        if self._connect_error is not None:
            raise self._connect_error
        self.entered += 1
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    async def get_metadata(self, paths: Iterable[str], *args: Any, **kwargs: Any) -> dict[str, Any]:
        requested = list(paths)
        self.metadata_calls.append(requested)
        missing = [p for p in requested if p not in self._known]
        if missing:
            if self._metadata_batch_error is not None and len(requested) > 1:
                raise self._metadata_batch_error
            raise KeyError(f"unknown path(s): {', '.join(missing)}")
        return {p: object() for p in requested}

    async def set(self, updates: Any, try_v2: bool = False, **kwargs: Any) -> None:
        if self._set_error is not None:
            raise self._set_error
        self.set_calls.append(list(updates))

    async def get_target_values(self, paths: Iterable[str], **kwargs: Any) -> dict[str, Any]:
        return {p: v for p, v in self._target_values.items() if p in set(paths)}

    async def subscribe_target_values(
        self, paths: Iterable[str], **kwargs: Any
    ) -> AsyncIterator[dict[str, Any]]:
        """The v2 path: `OpenProviderStream` actuation requests.

        On kuksa-client 0.5.2 this only falls back to v1 when the databroker
        answers UNIMPLEMENTED. Against a broker that DOES serve v2 — 0.7.x and
        later — it stays on v2 and never sees a v1 writer's targets.
        """
        if self._subscribe_error is not None:
            raise self._subscribe_error
        for update in self._target_updates:
            yield update
            await asyncio.sleep(0)
        # A real stream stays open. Without this, the v2 branch returning
        # promptly would let a test pass on the v1 branch alone.
        while True:
            await asyncio.sleep(0)

    async def subscribe(
        self, entries: Iterable[Any], try_v2: bool = False, **kwargs: Any
    ) -> AsyncIterator[list[Any]]:
        """The v1 path: `Subscribe` with View.TARGET_VALUE.

        Yields lists of update objects carrying `.entry.path` and
        `.entry.actuator_target`, matching the real v1 stream shape.
        """
        recorded = list(entries)
        self.subscribe_entries.append(recorded)
        if self._v1_subscribe_error is not None:
            raise self._v1_subscribe_error
        for update in self._v1_target_updates:
            yield [_V1Update(path, dp) for path, dp in update.items()]
            await asyncio.sleep(0)
        while True:
            await asyncio.sleep(0)


# ── helpers ──────────────────────────────────────────────────────────────────


@dataclass
class _V1Entry:
    path: str
    actuator_target: Any


@dataclass
class _V1Update:
    """One element of a v1 Subscribe stream: `update.entry.actuator_target`."""

    entry: _V1Entry

    def __init__(self, path: str, actuator_target: Any) -> None:
        self.entry = _V1Entry(path, actuator_target)


def signal(namespace: str, name: str, value: Any) -> Signal:
    return Signal(name=name, namespace=namespace, value=value)


class RecordingSleep:
    """An injectable asyncio.sleep that records delays instead of waiting."""

    def __init__(self, stop_after: int | None = None) -> None:
        self.delays: list[float] = []
        self._stop_after = stop_after

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)
        if self._stop_after is not None and len(self.delays) >= self._stop_after:
            raise asyncio.CancelledError
        await asyncio.sleep(0)
