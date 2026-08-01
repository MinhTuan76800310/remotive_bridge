#!/usr/bin/env python3
"""Print a mapping skeleton for a running vCar — with the port already right.

    ./scripts/discover_mapping.py                       # auto-find the broker
    ./scripts/discover_mapping.py --url http://127.0.0.1:50105
    ./scripts/discover_mapping.py > my-vehicle.yaml

Why this exists
---------------
`mapping.example.yaml` hard-codes `127.0.0.1:50051`, which is correct for the
hand-written `vss-vcar` rig and WRONG for every vCar kx360v generates. Those get
a port allocated from `GRPC_PORT_BASE = 50100` upward (`app/config.py`), so the
first is 50100, the next 50101, and so on — and the number changes when the
instance is rebuilt. A mapping copied from the example then fails with

    Connection refused (111)   ... on a port where nothing is listening

which reads like a broken bridge and is really a wrong number.

The signal names differ too. The example names `BCM-VehicleCAN` and
`VSS_VehicleState.*` because that is what the `vss-vcar` DBC contains. A vCar
built from Atlas has namespaces of the form `{ECU}-{ChannelKey}` and whatever
messages its communication matrix declares. Guessing them produces a mapping
that connects and then drops every entry.

So: ask the broker. Everything printed below was read from the live vehicle.

This is a *skeleton*, not a finished mapping. It lists what exists; you still
choose which signals to bridge, which VSS path each becomes, and the transform.
Lines needing a decision are marked TODO and the bridge will refuse to start
while any remain — deliberately, so a half-edited file cannot be run by mistake.
"""

from __future__ import annotations

import argparse
import asyncio
import shutil
import subprocess
import sys

try:
    from remotivelabs.broker import BrokerClient
except ImportError:  # pragma: no cover - a clearer message than the traceback
    sys.exit("remotivelabs-broker is not installed. Run: uv sync")

# kx360v allocates from here upward; see app/config.py:18.
GRPC_PORT_BASE = 50100
# Enough to cover MAX_CONCURRENT_VCARS several times over without being slow.
SCAN_PORTS = 24
# The hand-written vss-vcar rig, which does not follow the allocation rule.
LEGACY_PORT = 50051


def _ports_from_docker() -> list[int]:
    """Host ports published as container 50051, newest first.

    Reading it from Docker beats scanning: it names the container too, so the
    output says *which* vCar was found rather than just a number.
    """
    if not shutil.which("docker"):
        return []
    try:
        out = subprocess.run(
            ["docker", "ps", "--filter", "publish=50051",
             "--format", "{{.Names}}\t{{.Ports}}"],
            capture_output=True, text=True, timeout=10,
        ).stdout
    except (subprocess.SubprocessError, OSError):
        return []

    found: list[int] = []
    for line in out.splitlines():
        name, _, ports = line.partition("\t")
        for chunk in ports.split(","):
            # "127.0.0.1:50105->50051/tcp"
            if "->50051/tcp" not in chunk:
                continue
            hostpart = chunk.split("->")[0].strip()
            try:
                port = int(hostpart.rsplit(":", 1)[-1])
            except ValueError:
                continue
            print(f"# found {name} on host port {port}", file=sys.stderr)
            found.append(port)
    return found


async def _probe(url: str, timeout: float) -> list | None:
    """Every frame on the vehicle, or None if no broker answered.

    `list_frame_infos()` with no arguments returns an EMPTY list, not everything
    — measured on remotivelabs-broker 0.9.1 against a broker holding 2 frames.
    It answers instantly and without error, so a caller that trusts it sees a
    vehicle with no signals and concludes the topology is still building. The
    namespaces have to be enumerated first and passed in.
    """
    try:
        async with asyncio.timeout(timeout):
            async with BrokerClient(url=url, client_id="discover") as broker:
                namespaces = [ns.name for ns in await broker.list_namespaces()]
                if not namespaces:
                    return []
                return await broker.list_frame_infos(*namespaces)
    except Exception:
        return None


async def _find(explicit: str | None, timeout: float) -> tuple[str, list]:
    if explicit:
        infos = await _probe(explicit, timeout)
        if infos is None:
            sys.exit(f"error: no broker answered at {explicit}")
        return explicit, infos

    candidates = _ports_from_docker()
    candidates += [p for p in range(GRPC_PORT_BASE, GRPC_PORT_BASE + SCAN_PORTS)
                   if p not in candidates]
    if LEGACY_PORT not in candidates:
        candidates.append(LEGACY_PORT)

    for port in candidates:
        url = f"http://127.0.0.1:{port}"
        infos = await _probe(url, timeout)
        # `is not None` — an empty list means the broker answered but has no
        # signals yet, which is a different problem from nothing listening, and
        # the caller reports each differently.
        if infos is not None:
            return url, infos

    sys.exit(
        "error: no broker found.\n"
        f"  Scanned 127.0.0.1 ports {GRPC_PORT_BASE}-{GRPC_PORT_BASE + SCAN_PORTS - 1} "
        f"and {LEGACY_PORT}.\n"
        "  If the vCar is on another host, or the bridge will run inside the vCar\n"
        "  network, pass --url explicitly.\n"
        "  The published port is the LEFT side of '...->50051/tcp' in `docker ps`."
    )


def _emit(url: str, infos: list, kuksa_host: str, kuksa_port: int) -> None:
    out = sys.stdout.write

    out("# Generated by scripts/discover_mapping.py — read from the live vehicle.\n")
    out("#\n")
    out("# Every namespace, frame and signal below EXISTS. What is not decided is\n")
    out("# which of them you want, and what each becomes in VSS. Fill in the TODOs.\n")
    out("\n")
    out("remotive:\n")
    out(f"  url: {url}\n")
    out("  # From the HOST this port is correct until the instance is rebuilt.\n")
    out("  # From a container beside the vCar, use the service name and the\n")
    out("  # container-side port instead — that survives a rebuild:\n")
    out("  #   url: http://topology-broker.com:50051\n")
    out("  # and join the network: BRIDGE_NETWORK=<project>_----control_network\n")
    out("\n")
    out("kuksa:\n")
    out(f"  host: {kuksa_host}\n")
    out(f"  port: {kuksa_port}\n")
    out("\n")
    out("options:\n")
    out("  seed_seconds: 3\n")
    out("  retry_delay: 10\n")
    out("  health_host: 0.0.0.0\n")
    out("  health_port: 8090\n")
    out("\n")

    readable = [i for i in infos if i.signals]
    if not readable:
        out("# The broker reported no signals. Has the topology finished building?\n")
        return

    out("# ── Remotive -> VSS : current values ───────────────────────────────────\n")
    out("#\n")
    out("# `type` is MANDATORY. Without it kuksa-client fetches the type before\n")
    out("# every write, and Datapoint(\"0\") is truthy — an untyped CAN 0 would\n")
    out("# invert every boolean.\n")
    out("to_vss:\n")

    for info in sorted(readable, key=lambda i: (i.namespace, i.name)):
        cycle = info.cycle_time_millis
        senders = ", ".join(info.sender) or "none"
        out(f"\n  # {info.name} on {info.namespace}"
            f"  (cycle {cycle:g} ms, sender: {senders})\n")
        if not cycle:
            out("  # ! No cycle time: seeding cannot reach this frame and on_change\n")
            out("  #   may never fire. Bridging it is likely to look silent.\n")
        for signal in sorted(info.signals):
            qualified = signal if "." in signal else f"{info.name}.{signal}"
            out(f"  # - can: {{namespace: {info.namespace}, signal: {qualified}}}\n")
            out("  #   vss: TODO.Vehicle.Path\n")
            out("  #   type: TODO   # boolean | string | int | float\n")

    out("\n")
    out("# ── VSS -> Remotive : actuation targets ────────────────────────────────\n")
    out("#\n")
    out("# Two measured warnings before you uncomment anything here:\n")
    out("#\n")
    out("# F9  Writing a frame an ECU ALSO transmits makes the bridge a second\n")
    out("#     transmitter; the value alternates at cycle rate. Frames with a\n")
    out("#     sender listed above are exactly those. Prefer a frame nobody drives.\n")
    out("# F11 cpd-core 1.0.0 writes targets over KUKSA v1 while the bridge reads\n")
    out("#     them over v2. The two never meet, silently. This direction will not\n")
    out("#     fire for CPD until that is closed.\n")
    out("to_can: []\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Print a mapping skeleton from a live RemotiveLabs broker.",
    )
    parser.add_argument("--url", help="broker URL; omit to auto-detect")
    parser.add_argument("--kuksa-host", default="127.0.0.1")
    parser.add_argument("--kuksa-port", type=int, default=55555)
    parser.add_argument("--timeout", type=float, default=4.0,
                        help="per-port probe timeout in seconds (default: 4)")
    args = parser.parse_args()

    url, infos = asyncio.run(_find(args.url, args.timeout))
    print(f"# broker: {url}  ({len(infos)} frames)", file=sys.stderr)
    _emit(url, infos, args.kuksa_host, args.kuksa_port)


if __name__ == "__main__":
    main()
