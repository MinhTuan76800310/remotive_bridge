"""Spike: settle risks F1 and F6 against a live vCar.

Not bridge code. This exists so the two unproven assumptions in the design are
decided by measurement rather than by reading a proto comment, and so the next
person can re-run the experiment instead of re-deriving it from prose.

**F1 — is `Restbus.add()` destructive across clients?**
The generated stub says *"Add removes any previous configuration before applying
the new one."* Whether "previous configuration" is scoped per-client or
per-namespace is undocumented and the broker is closed-source. If it is
per-namespace, a bridge that calls `add()` on a namespace an ECU owns silently
stops that ECU transmitting — and `allow_add` must default to false.

**F6 — does a restbus write reach the bus at all?**
`kx360v-management` records that a write to a frame an ECU transmits "never
reaches the bus". RemotiveLabs' own example contradicts the general rule. The
measurement that produced that claim used the worst possible subject, so it is
re-run here on a frame with a real cycle time.

Method notes that matter:

* **Observation always uses a third namespace.** CAN loopback filtering can
  suppress a frame in the transmitting ECU's own namespace, so reading back
  there proves nothing either way.
* **Detection is by sentinel value**, not by frame arrival. The ECU is
  transmitting the same frame at 100 Hz throughout; the only reliable evidence
  that *our* write landed is a value the vehicle never produces on its own.
* **The F1 probe runs last**, because if F1 is real it breaks the rig.

Usage (needs a running vCar):

    uv run python scripts/spike_restbus.py --url http://127.0.0.1:50051
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from collections import Counter
from typing import Any

from remotivelabs.broker import BrokerClient
from remotivelabs.broker.restbus import RestbusFrameConfig, RestbusSignalConfig

# The rig: models/bcm and models/vc in bridge/vss-vcar.
OBSERVE_NS = "topology-VehicleCAN"  # owned by no ECU model
BCM_NS = "BCM-VehicleCAN"  # BCM runs a restbus here
STATE_FRAME = "VSS_VehicleState"  # sender BCM, 100 ms
HMI_FRAME = "VC_To_HMI"  # sender VC, 100 ms
LV_SIGNAL = f"{STATE_FRAME}.Vehicle_LowVoltageSystemState"
TELLTALE_SIGNAL = f"{HMI_FRAME}.TelltaleId"

# LowVoltageSystemState 5 = START. The BCM scenario only ever emits 4 (ON) and
# 2 (OFF), so a 5 arriving at the observer can only have come from this script.
SENTINEL_LV = 5
# TelltaleId 2 = HAZARD. The VC decision table only produces 0 and 1.
SENTINEL_TELLTALE = 2


class Observer:
    """Counts frames and records values seen on a namespace we do not write to."""

    def __init__(self, client: BrokerClient, namespace: str, signals: list[str]) -> None:
        self._client = client
        self._namespace = namespace
        self._signals = signals
        self._task: asyncio.Task[None] | None = None
        self.counts: Counter[str] = Counter()
        self.values: dict[str, Counter[Any]] = {s: Counter() for s in signals}

    async def __aenter__(self) -> Observer:
        # on_change=False: we are measuring cycle rate, and a static signal
        # would look identical to a stopped one under on_change=True.
        stream = await self._client.subscribe(
            (self._namespace, self._signals), on_change=False
        )
        self._task = asyncio.create_task(self._consume(stream))
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _consume(self, stream: Any) -> None:
        async for batch in stream:
            for signal in batch:
                self.counts[signal.name] += 1
                self.values[signal.name][signal.value] += 1

    def window(self) -> dict[str, Any]:
        """Take and reset a measurement window."""
        result = {
            "counts": dict(self.counts),
            "values": {k: dict(v) for k, v in self.values.items() if v},
        }
        self.counts.clear()
        for counter in self.values.values():
            counter.clear()
        return result


async def _observe_for(observer: Observer, seconds: float) -> dict[str, Any]:
    observer.window()  # discard whatever accumulated before the window opened
    await asyncio.sleep(seconds)
    return observer.window()


def _saw(window: dict[str, Any], signal: str, value: Any) -> bool:
    return value in window.get("values", {}).get(signal, {})


async def run(url: str, window_s: float) -> dict[str, Any]:
    report: dict[str, Any] = {
        "url": url,
        "window_s": window_s,
        "observe_namespace": OBSERVE_NS,
    }

    # Separate connections: a single client could mask a per-client scoping rule,
    # which is precisely what F1 is about.
    async with BrokerClient(url=url, client_id="spike-observer") as observer_client, \
            BrokerClient(url=url, client_id="spike-writer") as writer:

        frames = {
            f.name: f for f in await writer.list_frame_infos(BCM_NS)
        }
        report["frames"] = {
            name: {"cycle_ms": f.cycle_time_millis, "sender": list(f.sender)}
            for name, f in frames.items()
        }
        # F6's premise is that the frame is transmitted cyclically at all.
        report["preflight_cyclic"] = all(
            frames[n].cycle_time_millis > 0 for n in (STATE_FRAME, HMI_FRAME) if n in frames
        )

        async with Observer(
            observer_client, OBSERVE_NS, [LV_SIGNAL, TELLTALE_SIGNAL]
        ) as observer:

            # ── 0. baseline ──────────────────────────────────────────────────
            report["baseline"] = await _observe_for(observer, window_s)

            # ── 1. F6: update with no add(), on the namespace BCM owns ───────
            await writer.restbus.update_signals(
                (BCM_NS, [RestbusSignalConfig.set(LV_SIGNAL, SENTINEL_LV)])
            )
            window = await _observe_for(observer, window_s)
            report["update_only"] = {
                "namespace": BCM_NS,
                "wrote": {LV_SIGNAL: SENTINEL_LV},
                "observed": window,
                "sentinel_reached_bus": _saw(window, LV_SIGNAL, SENTINEL_LV),
            }

            # ── 2. F6: update with no add(), on the topology namespace ───────
            # A namespace no ECU model owns — the least contended path a bridge
            # could take.
            await writer.restbus.update_signals(
                (OBSERVE_NS, [RestbusSignalConfig.set(TELLTALE_SIGNAL, SENTINEL_TELLTALE)])
            )
            window = await _observe_for(observer, window_s)
            report["update_only_topology_ns"] = {
                "namespace": OBSERVE_NS,
                "wrote": {TELLTALE_SIGNAL: SENTINEL_TELLTALE},
                "observed": window,
                "sentinel_reached_bus": _saw(window, TELLTALE_SIGNAL, SENTINEL_TELLTALE),
            }

            # ── 3. F1: add a frame to a namespace an ECU already drives ──────
            # BCM's restbus on BCM_NS holds STATE_FRAME (SenderFilter BCM).
            # Adding a *different* frame is the cleanest probe: if Add wipes the
            # namespace, BCM's own frame stops cycling and we see it here.
            #
            # This is the destructive step, so it runs last.
            hmi_cycle = frames[HMI_FRAME].cycle_time_millis if HMI_FRAME in frames else 100.0
            await writer.restbus.add(
                (BCM_NS, [RestbusFrameConfig(name=HMI_FRAME, cycle_time=hmi_cycle)]),
                start=True,
            )
            window = await _observe_for(observer, window_s)
            state_still_cycling = window["counts"].get(LV_SIGNAL, 0) > 0
            report["add_scope"] = {
                "namespace": BCM_NS,
                "added_frame": HMI_FRAME,
                "observed": window,
                "ecu_frame_still_cycling": state_still_cycling,
                "f1_confirmed": not state_still_cycling,
            }

            # ── 4. F6: the design's actual path, add then update ─────────────
            await writer.restbus.update_signals(
                (BCM_NS, [RestbusSignalConfig.set(TELLTALE_SIGNAL, SENTINEL_TELLTALE)])
            )
            window = await _observe_for(observer, window_s)
            report["add_then_update"] = {
                "namespace": BCM_NS,
                "wrote": {TELLTALE_SIGNAL: SENTINEL_TELLTALE},
                "observed": window,
                "sentinel_reached_bus": _saw(window, TELLTALE_SIGNAL, SENTINEL_TELLTALE),
            }

    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:50051")
    parser.add_argument(
        "--window",
        type=float,
        default=3.0,
        help="seconds to observe per phase; 3 s is ~30 frames at 100 ms",
    )
    args = parser.parse_args()
    print(json.dumps(asyncio.run(run(args.url, args.window)), indent=2, default=str))


if __name__ == "__main__":
    main()
