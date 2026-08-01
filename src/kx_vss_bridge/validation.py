"""Cross-check the parsed mapping against what the peers actually contain.

`config.py` can tell that `signal: BareName` is malformed. It cannot tell that
`VSS_VehicleState.Vehicle_Body_Horn_IsActive` is absent from this vehicle, or
that `Vehicle.Cabin.HMI.ChimeId` is missing from the databroker's catalog. Only a
live connection knows that, so this runs on every Remotive connection and its
result replaces the previous one.

Two outcomes:

* **Dropped** — the signal or path does not exist. The entry cannot work, so it
  is removed and the reason recorded. Its siblings keep running.
* **Warned** — it exists, but will behave in a way the operator would not
  predict. Nothing is blocked.

The two warnings that matter were measured against a live rig on 2026-08-01, not
inferred (`bridge/docs/spike-f1-f6-findings.md`):

* **F9.** Writing a frame an ECU also transmits makes the bridge a *second*
  transmitter. Both writes reach the bus and the receiver sees the value
  alternate at cycle rate. This is a mapping error only the operator can resolve.
* **F10.** `update_signals` on a namespace whose restbus holds no such frame is
  silently ignored — no error, no delivery. With `allow_add: false` and no ECU
  driving the frame, the mapping is inert.

Both were once believed to be reasons to refuse a mapping. Measurement says
otherwise, and §3 of the design is a worked example of what happens when an
unverified prohibition is encoded as a rule.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

import structlog
from remotivelabs.broker import FrameInfo
from remotivelabs.broker.restbus import RestbusFrameConfig

from kx_vss_bridge.config import BridgeConfig, Mapping

__all__ = ["ValidatedMapping", "validate_mapping"]

log = structlog.get_logger(__name__)


class _Broker(Protocol):
    async def list_frame_infos(self, *namespaces: str) -> list[FrameInfo]: ...


class _Kuksa(Protocol):
    async def get_metadata(self, paths: Any, *args: Any, **kwargs: Any) -> dict[str, Any]: ...


@dataclass(frozen=True)
class ValidatedMapping:
    """What survived validation, plus everything the operator should know."""

    to_vss: tuple[Mapping, ...]
    to_can: tuple[Mapping, ...]
    # (namespace, [qualified signal]) — ready to splat into client.subscribe().
    subscription_groups: tuple[tuple[str, list[str]], ...]
    # namespace -> frame -> config, deduplicated. One add() call covers all of it.
    restbus_frames: dict[str, dict[str, RestbusFrameConfig]]
    skipped: list[dict[str, str]]
    warnings: list[dict[str, Any]]

    @property
    def active_to_vss(self) -> int:
        return len(self.to_vss)

    @property
    def active_to_can(self) -> int:
        return len(self.to_can)


def _index_frames(infos: list[FrameInfo]) -> dict[tuple[str, str], tuple[FrameInfo, str]]:
    """Map (namespace, qualified signal) -> (frame, signal).

    The live broker returns already-qualified keys — verified against the running
    rig, where `list_frame_infos` gives
    'VSS_VehicleState.Vehicle_Body_Horn_IsActive'. Older versions return bare
    names, so both are indexed; blindly re-prefixing would lose the qualified
    form entirely.
    """
    index: dict[tuple[str, str], tuple[FrameInfo, str]] = {}
    for info in infos:
        for signal_name in info.signals:
            qualified = (
                signal_name if "." in signal_name else f"{info.name}.{signal_name}"
            )
            index[(info.namespace, qualified)] = (info, qualified)
    return index


async def _resolve_vss_paths(kuksa: _Kuksa, paths: tuple[str, ...]) -> set[str]:
    """Which VSS paths the databroker knows.

    One batched call in the good case. If it fails — the broker rejects the whole
    request when any path is unknown — fall back to probing each path, so a
    single typo does not take every mapping down with it.
    """
    if not paths:
        return set()
    try:
        return set(await kuksa.get_metadata(list(paths)))
    except Exception as exc:
        log.debug("batched metadata lookup failed; probing individually", error=str(exc))

    known: set[str] = set()
    for path in paths:
        try:
            await kuksa.get_metadata([path])
            known.add(path)
        except Exception:
            pass  # absent from the catalog; the caller records why it was dropped
    return known


async def validate_mapping(
    config: BridgeConfig, broker: _Broker, kuksa: _Kuksa
) -> ValidatedMapping:
    frame_index = _index_frames(await broker.list_frame_infos(*config.all_namespaces))
    live_namespaces = {namespace for namespace, _ in frame_index}
    known_paths = await _resolve_vss_paths(kuksa, config.all_vss_paths)

    kept: dict[str, list[Mapping]] = {"to_vss": [], "to_can": []}
    resolved: dict[int, FrameInfo] = {}  # id(mapping) -> its frame
    skipped: list[dict[str, str]] = []
    warnings: list[dict[str, Any]] = []
    warned_frames: set[tuple[str, str]] = set()

    for section, mappings in (("to_vss", config.to_vss), ("to_can", config.to_can)):
        for mapping in mappings:
            label = f"{mapping.can.namespace}:{mapping.can.signal} <-> {mapping.vss}"

            if mapping.can.namespace not in live_namespaces:
                skipped.append({
                    "entry": label,
                    "reason": f"namespace {mapping.can.namespace!r} not present in the vehicle",
                })
                continue

            found = frame_index.get((mapping.can.namespace, mapping.can.signal))
            if found is None:
                skipped.append({
                    "entry": label,
                    "reason": f"signal {mapping.can.signal!r} not in {mapping.can.namespace!r}",
                })
                continue

            if mapping.vss not in known_paths:
                skipped.append({
                    "entry": label,
                    "reason": f"VSS path {mapping.vss!r} not in the databroker catalog",
                })
                continue

            info, _ = found
            resolved[id(mapping)] = info
            kept[section].append(mapping)

            if section == "to_vss" and info.cycle_time_millis == 0:
                # Seeding opens with on_change=False and waits for cyclic frames;
                # a frame that is never transmitted will not appear, and
                # on_change may never fire either.
                warnings.append({
                    "frame": info.name,
                    "namespace": info.namespace,
                    "note": "frame has no cycle time; seeding cannot reach it and "
                            "on_change may never fire",
                })

            if section == "to_can":
                # Keyed by frame, not by signal: both conditions below are
                # properties of the frame, and a frame carrying eight mapped
                # signals would otherwise emit eight identical warnings and bury
                # everything else in /stats.
                frame_key = (info.namespace, info.name)
                if frame_key not in warned_frames:
                    senders = list(info.sender)
                    if senders:
                        warned_frames.add(frame_key)
                        warnings.append({
                            "frame": info.name,
                            "namespace": info.namespace,
                            "sender": senders,
                            "note": f"{', '.join(senders)} also transmits this frame; the bridge "
                                    "becomes a second transmitter and the value will alternate "
                                    "between them at cycle rate (F9, measured)",
                        })
                    elif not mapping.allow_add:
                        warned_frames.add(frame_key)
                        warnings.append({
                            "frame": info.name,
                            "namespace": info.namespace,
                            "note": "no ECU transmits this frame and allow_add is false, so "
                                    "update_signals will be silently ignored (F10, measured)",
                        })

    # Subscriptions: one entry per namespace, so the whole vehicle is covered by
    # a single gRPC stream.
    groups: dict[str, list[str]] = {}
    for mapping in kept["to_vss"]:
        groups.setdefault(mapping.can.namespace, []).append(mapping.can.signal)

    # Restbus: one entry per frame. add() is destructive per client, so every
    # frame must go in one call — never incrementally.
    restbus: dict[str, dict[str, RestbusFrameConfig]] = {}
    for mapping in kept["to_can"]:
        if not mapping.allow_add:
            continue
        info = resolved[id(mapping)]
        restbus.setdefault(mapping.can.namespace, {}).setdefault(
            info.name,
            # The real cycle time, not the SDK's 0.0 default — a frame with no
            # cycle time is not transmitted cyclically at all.
            RestbusFrameConfig(name=info.name, cycle_time=info.cycle_time_millis),
        )

    for entry in skipped:
        log.error("mapping dropped", **entry)
    for entry in warnings:
        log.warning("mapping warning", **entry)

    return ValidatedMapping(
        to_vss=tuple(kept["to_vss"]),
        to_can=tuple(kept["to_can"]),
        subscription_groups=tuple(groups.items()),
        restbus_frames=restbus,
        skipped=skipped,
        warnings=warnings,
    )
