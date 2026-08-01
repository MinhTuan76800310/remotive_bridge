# kx-vss-bridge

Maps RemotiveLabs CAN signals to Eclipse KUKSA VSS signals, 1-to-1, in both
directions. A client on both ends: it owns no state, embeds no databroker, and
publishes no signal of its own.

```
┌─ vCar (RemotiveLabs) ────────┐
│  ECUs ──── topology-broker   │
└──────────────┬──────────────┘
                │ gRPC 50051
         ┌──────┴───────┐
         │  vss-bridge  │ ←── mapping.yaml
         └──────┬───────┘
                │ gRPC 55555
┌──────────────┴──────────────┐
│  KUKSA databroker            │
│  VSS 6.0 + overlays          │
└──────────────┬──────────────┘
                │
     cpd-core, dashboards, …
```

| Direction | Reads | Writes |
|---|---|---|
| Remotive → VSS | CAN signals | VSS **current values** — what the vehicle reports |
| VSS → Remotive | VSS **actuation targets** — what a function commands | the CAN restbus |

The bridge commands; it does not perform. It sets the CAN value an ECU will read;
whether the ECU acts is the ECU's business.

---

## Running it without a checkout

Every push to `main` publishes a `linux/amd64` image to
`ghcr.io/minhtuan76800310/remotive_bridge:latest`
([`.github/workflows/docker-publish.yml`](.github/workflows/docker-publish.yml)).
The tests run first; a red suite publishes nothing.

[`run_remotive_vss_bridge.sh`](run_remotive_vss_bridge.sh) is the whole handout —
copy that one file to a machine with Docker and it works:

```bash
./run_remotive_vss_bridge.sh                  # the example mapping shipped in the image
./run_remotive_vss_bridge.sh my-vehicle.yaml  # your own, mounted read-only
./run_remotive_vss_bridge.sh -h               # options
```

It pulls `latest` on every run and runs with `--rm`, so you always get the
current bridge and nothing is left behind. **Ctrl+C is the correct way to stop
it** — the graceful path is what stops the restbus frames the bridge started
(see *Purity*, below).

The image ships `mapping.example.yaml` at
`/usr/share/kx-vss-bridge/mapping.example.yaml`, but *not* at the default config
path. Mount your own or the bridge starts against the example's signal names —
which is why the fallback is explicit in the script rather than baked into `CMD`.

| Variable | Default | Use when |
|---|---|---|
| `BRIDGE_NETWORK` | `host` | the peers are in containers: `BRIDGE_NETWORK=vss_hmi_vcar_----control_network` |
| `BRIDGE_HEALTH_PORT` | `8090` | published only on a non-host network |
| `BRIDGE_IMAGE` | `…/remotive_bridge:latest` | pinning a `sha-…` tag to roll back |
| `BRIDGE_PULL` | `always` | `never` to run the local image offline |
| `BRIDGE_ENGINE` | auto | forcing `podman` where both are installed |

The default is host networking because the example mapping points at
`127.0.0.1`. On a bridged network that address is the container itself, not your
peers — change both together.

No login is needed to pull: the package inherits this repository's public
visibility. Verified anonymously against the first published build — the registry
serves the manifest with a token carrying no credentials.

---

## Quick start

You need a running vCar and a running databroker. Both are in this repository:
`vss-vcar/` is a two-ECU rig, and the CPD package ships a databroker with the
overlay the example mapping expects.

```bash
# 1. Install
uv sync --dev

# 2. Start the vCar (needs an active RemotiveTopology subscription)
cd vss-vcar
remotive topology generate -f instances/main.instance.yaml build   # CLI ≥0.20: 'build'
cd build/vss_hmi_vcar && docker compose up --build -d && cd -

# 3. Confirm the databroker has the CPD overlay (port 55557 in the shipped stack)
#    A bare databroker on 55555 has only standard VSS 6.0; the three overlay
#    paths will be dropped at validation with a clear reason.

# 4. Run the bridge
.venv/bin/kx-vss-bridge --config mapping.example.yaml
```

Then watch it work:

```bash
curl -s localhost:8090/health | python3 -m json.tool
```

```json
{
  "status": "ok",
  "uptime_s": 5.7,
  "remotive": {"connected": true, "phase": "streaming", "batches": 31, "signals": 124},
  "kuksa":    {"connected": true, "phase": "writing"},
  "mapping":  {"to_vss": 4, "to_can": 2, "to_vss_writes": 124, "to_vss_drops": 0}
}
```

The rig runs a scenario loop, so `Vehicle.LowVoltageSystemState` and
`Vehicle.Cabin.ChildPresence.IsDetected` move on their own every 30 s:

```bash
docker run --rm --network host ghcr.io/eclipse-kuksa/kuksa-databroker-cli:main \
  --server 127.0.0.1:55557
# then:  get Vehicle.LowVoltageSystemState Vehicle.Cabin.ChildPresence.IsDetected
```

**Use `.venv/bin/kx-vss-bridge`, not `uv run kx-vss-bridge`.** `uv run` re-resolves
the project on each start and, in a non-interactive shell, can swallow the
process's output entirely — you get an empty log and a confusing exit code. The
console script has no such problem.

---

## Isolating the rig — only your changes move

By default the rig drives itself, which makes it useless for "I changed X,
therefore Y changed". Two things move signals on their own:

**BCM runs a scenario loop** — driving → parked → child left behind → child
removed, ~30 s per cycle — rewriting `ChildPresence.IsDetected` and
`LowVoltageSystemState` on a timer.

**VC reacts to BCM and transmits `VC_To_HMI` at 100 ms.** So even when quiet it is
a *second transmitter* on any frame you write from the VSS side, and the value
alternates between VC's and yours at cycle rate (finding F9).

`vss-vcar/isolate.override.yml` removes both:

```bash
cd vss-vcar/build/vss_hmi_vcar
docker compose -f docker-compose.yml -f ../../isolate.override.yml up -d
```

`BCM_SCENARIO=0` freezes the loop — BCM still provides the frame and serves its
restbus, it just stops driving the values. `vc` is scaled to 0, so nothing
contends for `VC_To_HMI`. Brokers and `topology-api` are untouched.

Verified baseline, nothing touched for 8 s:

```
CAN  VSS_VehicleState.Vehicle_LowVoltageSystemState -> [4]     stable
CAN  VC_To_HMI.TelltaleId                           -> [0]     stable
VSS  Vehicle.LowVoltageSystemState                  -> "ON"    stable
```

Then one change each way, and only that changed:

```
restbus update ...Vehicle_LowVoltageSystemState:1
  → VSS Vehicle.LowVoltageSystemState = LOCK

actuate.py Vehicle.Cabin.HMI.TelltaleId 2
  → CAN VC_To_HMI.TelltaleId = [2]          ← just 2, no alternation
```

Keep VC running (drop the `vc:` block) to watch the real CPD chain react instead —
but then expect `VC_To_HMI` to alternate.

### If signals still move on their own

**Restbus state outlives the writer.** A frame added to a namespace keeps being
transmitted cyclically by the *broker* even after the client that configured it
has gone. Deleting the `vc` container does not stop `VC_To_HMI`.

The bridge cleans up after itself on SIGTERM/SIGINT — it closes the restbus on
every namespace it added to, so a bus that was silent before it started is
silent again after it stops. Verified: 0 frames → 44 while running → 0 after
shutdown. A `kill -9` skips that, and leaves the frame cycling.

```bash
remotive broker restbus reset --url http://127.0.0.1:50051 --namespace BCM-VehicleCAN
remotive broker restbus reset --url http://127.0.0.1:50051 --namespace VC-VehicleCAN
remotive broker restbus reset --url http://127.0.0.1:50051 --namespace topology-VehicleCAN
```

`reset` only clears the *calling client's* configuration, so state added by a
process that has exited may survive it. The reliable clear is a full cycle:

```bash
docker compose -f docker-compose.yml -f ../../isolate.override.yml down
docker compose -f docker-compose.yml -f ../../isolate.override.yml up -d
```

**Use `down`/`up`, not `restart`.** Restarting the brokers together leaves the leaf
brokers unable to rejoin the topology broker — `remotive broker signals namespaces`
then shows only `topology-VehicleCAN` and `virt`, and the bridge correctly reports
every mapping skipped with *"namespace 'BCM-VehicleCAN' not present in the
vehicle"*. A full `down`/`up` restores the dependency ordering.

Check what the broker can actually see:

```bash
remotive broker signals namespaces --url http://127.0.0.1:50051
# ["virt", "topology-VehicleCAN", "VC-VehicleCAN", "BCM-VehicleCAN"]
```

And confirm nothing else is writing:

```bash
pgrep -af kx-vss-bridge     # a leaked bridge is a second writer
docker ps                   # bcm/vc running?
podman ps                   # cpd-core also commands actuators through the bridge
```

---

## Watching it work

Two things to change, two things to watch. Start the bridge first:

```bash
.venv/bin/kx-vss-bridge --config mapping.example.yaml
```

### The Remotive side: the broker dashboard

```bash
docker run -d --rm --name remotive-webapp -p 8088:8080 \
  remotivelabs/remotive-web-app:1.15
```

Open <http://localhost:8088> and connect it to **`http://localhost:50051`** — the
rig's `topology-api` serves gRPC-web there, which is what a browser needs. Then
subscribe to `VC_To_HMI.TelltaleId` and `VSS_VehicleState.*` on the
`topology-VehicleCAN` namespace.

**Note the port mapping: `8088:8080`, not `8088:80`.** The image `EXPOSE`s 80 but
its nginx actually listens on 8080, so publishing against 80 gives a connection
reset with the container apparently healthy.

**Use `topology-VehicleCAN`, not `BCM-VehicleCAN`.** It is owned by no ECU model,
so it sees everything on the bus. CAN loopback filtering can suppress a frame in
the transmitting ECU's own namespace, which makes a read there prove nothing.

Prefer a terminal? Same thing, no browser:

```bash
remotive broker signals subscribe --url http://127.0.0.1:50051 \
  --signal topology-VehicleCAN:VC_To_HMI.TelltaleId \
  --signal topology-VehicleCAN:VSS_VehicleState.Vehicle_LowVoltageSystemState \
  --on-change-only
```

There is also `remotive studio . --broker-url http://127.0.0.1:50051` from inside
`vss-vcar/` (port 57123), which adds the topology view on top of signal
inspection.

### The VSS side: databroker-cli

```bash
docker run --rm -it --network host \
  ghcr.io/eclipse-kuksa/kuksa-databroker-cli:main --server 127.0.0.1:55557
```

Then inside it:

```
subscribe Vehicle.LowVoltageSystemState Vehicle.Cabin.ChildPresence.IsDetected
```

---

### Remotive → VSS: change a CAN signal, watch VSS

Change it:

```bash
remotive broker restbus update --url http://127.0.0.1:50051 \
  --signal 'BCM-VehicleCAN:VSS_VehicleState.Vehicle_LowVoltageSystemState:5'
```

Watch `Vehicle.LowVoltageSystemState` in databroker-cli go to `"START"`. Verified:

```
before:  Vehicle.LowVoltageSystemState = OFF
         → restbus update ... :5
after:   Vehicle.LowVoltageSystemState = START
```

`5` is `START` in the DBC's `VAL_` table, and the bridge's `enum` transform turns
it into the string the VSS catalog declares. The rig's own scenario loop only ever
emits `2` (OFF) and `4` (ON), so a `START` cannot be a coincidence.

Other values to try: `1` LOCK · `2` OFF · `3` ACC · `4` ON. Or child presence,
which CPD reacts to:

```bash
remotive broker restbus update --url http://127.0.0.1:50051 \
  --signal 'BCM-VehicleCAN:VSS_VehicleState.Vehicle_Cabin_ChildPresence_IsDetected:1'
```

The BCM scenario loop rewrites these signals every ~30 s, so a manual value holds
only until the loop's next write. To keep it stable, set `BCM_SCENARIO=0` on the
`bcm` service and restart it.

### VSS → Remotive: command an actuator, watch CAN

```bash
.venv/bin/python scripts/actuate.py Vehicle.Cabin.HMI.TelltaleId 2
#  Vehicle.Cabin.HMI.TelltaleId = 2  (UINT16, delivered to provider)
```

Watch `VC_To_HMI.TelltaleId` in Studio start showing `2`. Verified — observed
counts over 14 s:

```
value 0:   8      ← VC's own
value 1:  98      ← VC's own
value 2:  80      ← ours, via the bridge
value 41: 29      ← cpd-core's telltale, also through the bridge
```

**Expect `2` to alternate with VC's values rather than replace them.** VC also
transmits `VC_To_HMI`, so the bridge is a second transmitter and both write every
cycle (finding F9). Seeing `2` at all is the proof; seeing it exclusively would
mean VC had stopped.

If a previous run's value is latched in the restbus, clear it:

```bash
remotive broker restbus reset --url http://127.0.0.1:50051 --namespace BCM-VehicleCAN
```

### Why `scripts/actuate.py` and not databroker-cli

`databroker-cli`'s `actuate`, and `kuksa-client`'s `set_target_values()`, both
write the **v1 Target Value** field rather than issuing a v2 *Actuation* request.
Both now work — the bridge subscribes over v1 and v2 at once (F11) — but only
`Actuate` gives you a straight answer about whether a provider was listening:

| | v1 `set_target_values` | v2 `Actuate` |
|---|---|---|
| bridge running | value reaches CAN | value reaches CAN |
| bridge **not** running | succeeds anyway, `get_target_values` reads it back, nothing happens | fails `UNAVAILABLE: Provider ... does not exist` |

Actuation is never buffered: it reaches a live provider or it is refused. A v1
write is stored either way, so a silent success tells you nothing. That is why
this script exists and why `scripts/verify_bridge.py` uses the same call —
see [`docs/spike-f1-f6-findings.md`](docs/spike-f1-f6-findings.md) (F11).

`scripts/actuate.py` issues the v2 `Actuate` call, and reads the path's declared
type first so `uint16` vs `int32` cannot bite you.

| Exit | Meaning |
|---|---|
| 0 | delivered to the provider |
| 1 | `no provider is registered` — bridge not running, or path absent from `to_can` |
| 2 | bad path or value |

### Both at once, automatically

```bash
.venv/bin/python scripts/verify_bridge.py
```

Checks both directions with sentinel values and exits 0 only if both pass. It does
not import the bridge — it talks to the two peers as any third party would, so a
PASS means the *deployed* bridge moved a value. Confirmed to fail, with a named
reason, when the bridge is stopped.

---

## Configuration

One file, one bridge instance. Nothing is discovered; nothing is ambient. Start
from [`mapping.example.yaml`](mapping.example.yaml) — it is a working file, not a
skeleton.

```yaml
remotive:
  url: http://topology-broker.com:50051

kuksa:
  host: kuksa-databroker
  port: 55557
  # token: /run/secrets/kuksa.jwt     # only if the databroker enforces auth

options:
  seed_seconds: 3      # read cyclically this long before switching to on-change
  retry_delay: 10      # backoff between reconnects, both peers
  health_host: 0.0.0.0
  health_port: 8090

to_vss:                # CAN → VSS current values
  - can:  {namespace: BCM-VehicleCAN, signal: VSS_VehicleState.Vehicle_LowVoltageSystemState}
    vss:  Vehicle.LowVoltageSystemState
    type: string
    transform:
      op: enum
      map: {0: UNDEFINED, 1: LOCK, 2: "OFF", 3: ACC, 4: "ON", 5: START}

to_can:                # VSS actuation targets → CAN
  - vss:  Vehicle.Cabin.HMI.TelltaleId
    can:  {namespace: BCM-VehicleCAN, signal: VC_To_HMI.TelltaleId}
    type: int
    range: {min: 0, max: 255}
    allow_add: true
```

### Three things that will bite you

**Quote `ON` and `OFF`.** YAML 1.1 reads bare `on`, `off`, `yes`, `no` as
booleans. Two of the six `LowVoltageSystemState` values are `ON` and `OFF`, so
written naturally they become `True`/`False`, the databroker gets a boolean where
the catalog declares a string, and CPD never triggers — with no error anywhere.
The bridge refuses this at startup and names the key.

**`type` is mandatory.** Two measured reasons on kuksa-client 0.5.2:
`Datapoint("0")` evaluates to `True` (only `"False"/"false"/"F"/"f"` are falsy),
so an untyped CAN `0` inverts every boolean; and without a declared type, `set()`
performs a metadata round-trip before *every* write.

**Signal names are message-qualified.** `VSS_VehicleState.Vehicle_Body_Horn_IsActive`,
not `Vehicle_Body_Horn_IsActive`. A bare name is a silent `NOT_FOUND` at runtime,
so the bridge rejects it at parse time. Find the real names with:

```python
async with BrokerClient(url="http://127.0.0.1:50051") as c:
    for f in await c.list_frame_infos("BCM-VehicleCAN"):
        print(f.name, f.cycle_time_millis, list(f.signals))
```

**Pass the namespaces.** `list_frame_infos()` with no arguments returns an EMPTY
list, not everything — measured on remotivelabs-broker 0.9.1 against a broker
holding 2 frames. It answers instantly and without error, so a caller that trusts
it sees a vehicle with no signals and concludes the topology is still building.
Enumerate with `list_namespaces()` first, or just run
`./scripts/discover_mapping.py`, which does it for you.

### Reference

| Field | Meaning |
|---|---|
| `type` | `boolean` · `string` · `int` · `float`. Mandatory. |
| `transform.op` | `passthrough` · `linear` (`scale`, `offset`) · `threshold` (`gt`, `true_value`, `false_value`) · `enum` (`map`) |
| `range` | `{min, max}`, checked by the bridge on the value being sent. kuksa-client does not range-check: `12.7` into a `UINT8` silently becomes `12`. |
| `allow_add` | default `true`. Whether the bridge may add this frame to the restbus. |

Every transform has an inverse, because the VSS→CAN direction runs the mapping
backwards. `enum` inverts by reverse lookup, so two keys mapping to one value is
rejected as non-invertible. `threshold` discards magnitude, so it inverts to
`true_value`/`false_value` rather than pretending to reconstruct the input.

---

## What happens at startup

The mapping is checked against what the peers actually contain, on every Remotive
connection.

| Finding | Action |
|---|---|
| Signal or namespace absent from the vehicle | **dropped**, reason in `/stats.mapping.skipped` |
| VSS path absent from the databroker catalog | **dropped**, same |
| Frame has no cycle time (`to_vss`) | warned — seeding cannot reach it |
| An ECU also transmits this frame (`to_can`) | warned — see F9 below |
| `allow_add: false` and no ECU drives the frame | warned — see F10 below |

A **malformed entry** is dropped and reported; the rest run. A **structural**
problem — unreadable file, broken peer section, nothing usable at all — exits
non-zero, because starting would only hide it behind a healthy-looking container.

---

## Measured RemotiveLabs behaviour

Four findings from a live rig, 2026-08-01. Full method and output in
[`docs/spike-f1-f6-findings.md`](docs/spike-f1-f6-findings.md).

**`add()` is destructive per client, not per namespace.** Adding a frame to a
namespace where an ECU runs its own restbus left that ECU's frames cycling
untouched. So `allow_add: true` is a safe default. Every frame still goes in one
`add()` call, because `Add` *does* remove the calling client's previous
configuration — adding incrementally would erase the earlier frames.

**A restbus write does reach the bus**, even on a frame an ECU transmits, and even
with no `add()` at all. `app/signals/service.py` in `kx360v-management` records the
opposite; that measurement used `{ECU}_DUMMY`, where two signals share one 8-byte
frame and the stub echoes at flood rate — a self-feeding loop, not a property of
frames.

**F9 — writing a frame an ECU also transmits makes you a second transmitter.**
Both writes land; the receiver sees the value alternate at cycle rate. Measured:
frame rate doubled, values split evenly between the two writers. Validation warns.
On a production vehicle, write frames no ECU already drives.

**F10 — `update_signals` on a namespace whose restbus holds no such frame is
silently ignored.** No error, nothing delivered. `UpdateRequest` carries no client
id, so there is nothing to catch. Either target a namespace an ECU drives, or let
`allow_add` add the frame first.

---

## Running as a container

For the published image, use [`run_remotive_vss_bridge.sh`](run_remotive_vss_bridge.sh)
(above). To build and run your own:

```bash
docker build -t kx-vss-bridge:0.1.0 .

docker run --rm \
  --network "vcar-${VAC_NAME}_----control_network" \
  -v "$PWD/mapping.yaml:/config/mapping.yaml:ro" \
  -p 8090:8090 \
  kx-vss-bridge:0.1.0
```

The script runs the same image the same way; `BRIDGE_IMAGE=kx-vss-bridge:0.1.0
BRIDGE_PULL=never ./run_remotive_vss_bridge.sh mapping.yaml` uses a local build.

The network name is `vcar-{vac_name}` + `_----control_network` — four hyphens,
the topology builder's own convention (`app/instance/broker_addr.py:41`). For the
`vss-vcar` rig it is `vss_hmi_vcar_----control_network`.

**From a container, use the service name and the container-side port:**
`http://topology-broker.com:50051`. The host-published port is bound to
`127.0.0.1` and is unreachable from any container — and the host port changes
across rebuilds while the service name does not.

If the databroker is also containerised, the bridge needs a network that reaches
it too. Joining the vCar network alone will not resolve an external
`kuksa-databroker` hostname.

---

## Operating it

`GET /health` returns **200 whenever the process can answer**; degradation is in
the body. This is deliberate — a bridge waiting for a peer is working as designed,
and an orchestrator that restarted it on a 503 would destroy the retry behaviour.

`status` is `ok` only when both peers are connected *and* nothing was skipped.

`GET /stats` adds per-entry drop reasons and validation warnings.

The bridge never exits on a peer failure. Each direction is two independent
workers joined by a bounded latest-value buffer, so:

- KUKSA down → the reader keeps consuming CAN; newer values replace older ones per
  path; the whole snapshot is flushed on reconnect.
- Broker down → the target reader keeps consuming VSS; frames are re-added and the
  snapshot flushed on reconnect.
- Either restarting → validation re-runs, so a rebuilt vehicle is picked up.

Shutdown is graceful on SIGTERM and SIGINT: tasks are cancelled, the health socket
and both gRPC connections are closed.

---

## Development

```bash
uv sync --dev
uv run pytest -q          # 237 tests, no vCar or databroker needed
```

The suite uses fake peers throughout. Both loops and validation were additionally
verified against the live rig; the transform, state, validation and loop modules
were mutation-tested by injecting known bugs and confirming each was caught.

```
src/kx_vss_bridge/
  __main__.py         CLI, lifecycle, signal handling
  config.py           parse + validate the mapping into immutable indexes
  transform.py        coercion, transforms, inverses, range checks (pure)
  state.py            counters + the two bounded hand-off buffers
  validation.py       cross-check the mapping against both live peers
  remotive_to_vss.py  Loop A: broker reader ‖ KUKSA current-value writer
  vss_to_remotive.py  Loop B: KUKSA target reader ‖ restbus writer
  health.py           /health + /stats
scripts/
  spike_restbus.py    the F1/F6 experiment, re-runnable
  discover_mapping.py print a mapping skeleton from the live vehicle
  actuate.py          command one actuator over v2 Actuate
  verify_bridge.py    prove both directions of a running bridge
```

Design: [`../docs/superpowers/specs/2026-08-01-vss-remotive-bridge-design.md`](../docs/superpowers/specs/2026-08-01-vss-remotive-bridge-design.md)
