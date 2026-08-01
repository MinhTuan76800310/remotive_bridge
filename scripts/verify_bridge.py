"""Prove both directions of a running bridge actually work.

Run this while the bridge is running. It does not import the bridge: it talks to
the two peers exactly as any third party would, so a PASS means the *deployed*
bridge moved a value, not that its own code agrees with itself.

    .venv/bin/python scripts/verify_bridge.py

Two checks, one per direction.

**Remotive → VSS.** Writes a sentinel onto CAN and waits for it to appear as a
VSS current value. The sentinel is a value the rig's own ECUs never emit, so
seeing it in VSS cannot be a coincidence — `LowVoltageSystemState = 5` (START)
when the BCM scenario only ever produces 2 (OFF) and 4 (ON).

**VSS → Remotive.** Sets a VSS actuation target and watches for it on the CAN
bus. Two subtleties, both learned by measurement (`docs/spike-f1-f6-findings.md`):

* Read back through a **third namespace** — `topology-VehicleCAN`, owned by no ECU
  model. CAN loopback filtering can suppress a frame in the transmitting ECU's own
  namespace, so a read there proves nothing either way.
* Expect the sentinel **intermittently, not continuously**. VC also transmits
  `VC_To_HMI`, so the bridge is a second transmitter and the two alternate at
  cycle rate (finding F9). One sighting is a pass; demanding every frame would
  fail a working bridge.

Exit code is 0 only if both directions pass.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections import Counter

import grpc
from grpc.aio import AioRpcError

from kuksa.val.v2 import types_pb2 as types_v2
from kuksa.val.v2 import val_pb2 as v2
from kuksa_client.grpc.aio import VSSClient
from remotivelabs.broker import BrokerClient
from remotivelabs.broker.restbus import RestbusSignalConfig

# ── the rig (bridge/vss-vcar) ────────────────────────────────────────────────
BCM_NS = "BCM-VehicleCAN"
OBSERVE_NS = "topology-VehicleCAN"  # owned by no ECU model

LV_SIGNAL = "VSS_VehicleState.Vehicle_LowVoltageSystemState"
LV_PATH = "Vehicle.LowVoltageSystemState"

TELLTALE_SIGNAL = "VC_To_HMI.TelltaleId"
TELLTALE_PATH = "Vehicle.Cabin.HMI.TelltaleId"

# Values the rig's ECUs never produce, so a sighting is unambiguous.
LV_SENTINEL_RAW = 5  # START; BCM emits only 2 and 4
LV_SENTINEL_VSS = "START"
# HAZARD and CHILD_PRESENCE-ish spare codes. VC emits only 0 and 1, so either is
# unambiguous; two of them so the check can pick one that differs from whatever
# target is already set.
TELLTALE_SENTINELS = (2, 3, 4, 5)

GREEN, RED, DIM, BOLD, OFF = "\033[32m", "\033[31m", "\033[2m", "\033[1m", "\033[0m"


def _say(message: str = "") -> None:
    print(message, flush=True)


def _verdict(passed: bool, label: str, detail: str) -> bool:
    mark = f"{GREEN}PASS{OFF}" if passed else f"{RED}FAIL{OFF}"
    _say(f"  [{mark}] {label}")
    _say(f"         {DIM}{detail}{OFF}")
    return passed


async def _await_vss(
    kuksa: VSSClient, path: str, expected: object, timeout: float
) -> tuple[bool, list[object]]:
    """Poll a current value until it equals `expected`, or time out."""
    seen: list[object] = []
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        values = await kuksa.get_current_values([path])
        datapoint = values.get(path)
        value = datapoint.value if datapoint else None
        if not seen or seen[-1] != value:
            seen.append(value)
        if value == expected:
            return True, seen
        await asyncio.sleep(0.25)
    return False, seen


async def check_remotive_to_vss(
    broker: BrokerClient, kuksa: VSSClient, timeout: float
) -> bool:
    _say(f"{BOLD}Remotive → VSS{OFF}  (CAN signal becomes a VSS current value)")

    before = await kuksa.get_current_values([LV_PATH])
    start = before[LV_PATH].value if before.get(LV_PATH) else None
    _say(f"         {DIM}{LV_PATH} is currently {start!r}{OFF}")

    # Write straight to the restbus, as an independent client. Measured: this
    # reaches the bus even though BCM transmits the frame (F6 refuted).
    await broker.restbus.update_signals(
        (BCM_NS, [RestbusSignalConfig.set(LV_SIGNAL, LV_SENTINEL_RAW)])
    )
    _say(f"         {DIM}wrote {LV_SIGNAL} = {LV_SENTINEL_RAW} on {BCM_NS}{OFF}")

    landed, seen = await _await_vss(kuksa, LV_PATH, LV_SENTINEL_VSS, timeout)
    return _verdict(
        landed,
        f"CAN {LV_SENTINEL_RAW} → VSS {LV_SENTINEL_VSS!r}",
        f"values seen: {seen}"
        if landed
        else f"sentinel never arrived; saw {seen}. Is the bridge running, and is "
        f"{LV_PATH} in its to_vss mapping?",
    )


async def _actuate(kuksa: VSSClient, path: str, value: int) -> None:
    """Command an actuator the way a real consumer must.

    `set_target_values()` is NOT this. It writes the v1 *Target Value*
    perspective — a stored field — while `subscribe_target_values()` registers a
    v2 provider on `OpenProviderStream` and waits for *Actuation* requests. The
    two never meet: measured on kuksa-client 0.5.2 against a live databroker,
    `set_target_values` stores a value that `get_target_values` reads back
    happily, and the registered provider receives nothing at all.

    v2 has no target-value perspective and v1 has no actuation, so a v2 provider
    can only be driven by `Actuate`. This is the call CPD and any other consumer
    must use to command the bridge.
    """
    request = v2.ActuateRequest(
        signal_id=types_v2.SignalID(path=path),
        # The `Value` oneof field must match the catalog's declared type exactly;
        # the broker rejects int32 for a uint16 path with INVALID_ARGUMENT.
        value=types_v2.Value(uint32=value),
    )
    await kuksa.client_stub_v2.Actuate(
        request, metadata=kuksa.generate_metadata_header(None)
    )


async def check_vss_to_remotive(
    broker: BrokerClient, kuksa: VSSClient, timeout: float
) -> bool:
    _say()
    _say(f"{BOLD}VSS → Remotive{OFF}  (VSS actuation target becomes a CAN value)")

    seen: Counter[object] = Counter()

    # Subscribe before writing, so a value that arrives immediately is not missed.
    stream = await broker.subscribe((OBSERVE_NS, [TELLTALE_SIGNAL]), on_change=False)

    async def observe() -> None:
        async for batch in stream:
            for sig in batch:
                seen[sig.value] += 1

    watcher = asyncio.create_task(observe())
    try:
        await asyncio.sleep(2.0)
        baseline = dict(seen)
        _say(f"         {DIM}baseline on {OBSERVE_NS}: {baseline}{OFF}")

        # Pick a sentinel the bus is not already carrying. A previous run of this
        # script leaves its own sentinel latched in the restbus, so a fixed
        # choice makes the second run unable to attribute a sighting to anything.
        sentinel = next((v for v in TELLTALE_SENTINELS if v not in baseline), None)
        if sentinel is None:
            return _verdict(
                False,
                "sentinel choice",
                f"{TELLTALE_SIGNAL} already carries every candidate "
                f"{TELLTALE_SENTINELS}; restart the vCar to clear the restbus",
            )

        seen.clear()
        try:
            await _actuate(kuksa, TELLTALE_PATH, sentinel)
        except AioRpcError as exc:
            # UNAVAILABLE "Provider ... does not exist" is the signature of no
            # bridge running: v2 actuation is never buffered, it is delivered to
            # a live provider or refused. Worth naming, because the raw gRPC
            # traceback buries the one fact that matters.
            if exc.code() is grpc.StatusCode.UNAVAILABLE:
                return _verdict(
                    False,
                    f"VSS actuation {sentinel} → CAN",
                    f"no provider is registered for {TELLTALE_PATH}. The bridge "
                    f"is not running, or {TELLTALE_PATH} is not in its to_can mapping.",
                )
            return _verdict(
                False,
                f"VSS actuation {sentinel} → CAN",
                f"Actuate was rejected: {exc.code().name} — {exc.details()}",
            )
        _say(f"         {DIM}actuated {TELLTALE_PATH} = {sentinel}{OFF}")

        await asyncio.sleep(timeout)
        after = dict(seen)
    finally:
        watcher.cancel()
        try:
            await watcher
        except asyncio.CancelledError:
            pass

    landed = sentinel in after
    others = {k: v for k, v in after.items() if k != sentinel}
    detail = f"observed on {OBSERVE_NS}: {after}"
    if landed and others:
        # Not a defect. Expected whenever an ECU also transmits the frame.
        detail += " — alternating with VC's own values, which is finding F9"
    elif not landed:
        detail = (
            f"sentinel never reached the bus; saw {after}. Is {TELLTALE_PATH} in the "
            f"to_can mapping, and did /stats report a to_can write?"
        )
    return _verdict(landed, f"VSS target {sentinel} → CAN", detail)


async def run(broker_url: str, kuksa_host: str, kuksa_port: int, timeout: float) -> int:
    _say(f"{BOLD}Verifying a running bridge{OFF}")
    _say(f"  {DIM}remotive {broker_url} · kuksa {kuksa_host}:{kuksa_port}{OFF}")
    _say()

    async with BrokerClient(url=broker_url, client_id="verify-bridge") as broker, \
            VSSClient(kuksa_host, kuksa_port) as kuksa:
        a = await check_remotive_to_vss(broker, kuksa, timeout)
        b = await check_vss_to_remotive(broker, kuksa, timeout)

    _say()
    if a and b:
        _say(f"{GREEN}{BOLD}Both directions working.{OFF}")
        return 0
    failed = [n for n, ok in (("Remotive → VSS", a), ("VSS → Remotive", b)) if not ok]
    _say(f"{RED}{BOLD}Not working: {', '.join(failed)}{OFF}")
    _say(f"{DIM}Check `curl -s localhost:8090/stats` for skipped mappings and drops.{OFF}")
    return 1


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--broker", default="http://127.0.0.1:50051")
    parser.add_argument("--kuksa-host", default="127.0.0.1")
    parser.add_argument("--kuksa-port", type=int, default=55557)
    parser.add_argument(
        "--timeout",
        type=float,
        default=8.0,
        help="seconds to wait for each direction (default: 8)",
    )
    args = parser.parse_args()
    sys.exit(
        asyncio.run(run(args.broker, args.kuksa_host, args.kuksa_port, args.timeout))
    )


if __name__ == "__main__":
    main()
