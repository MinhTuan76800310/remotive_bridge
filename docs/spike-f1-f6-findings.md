# Spike: F1 and F6 settled against a live vCar

Date: 2026-08-01
Rig: `bridge/vss-vcar` — 2 ECUs (BCM, VC), one CAN channel, both frames cyclic at 100 ms
Versions: RemotiveBroker 1.24.5, `remotivelabs-broker` 0.9.1, topology CLI 0.15.1 / generator 0.25
Command: `uv run python scripts/spike_restbus.py --url http://127.0.0.1:50051 --window 3`

Observation was always through `topology-VehicleCAN`, a third namespace owned by no
ECU model, because CAN loopback filtering can suppress a frame in the transmitting
ECU's own namespace. Detection was by sentinel value — `LowVoltageSystemState = 5`
(START) and `TelltaleId = 2` (HAZARD), neither of which the models ever emit — because
the frames cycle at 100 Hz regardless and frame arrival alone proves nothing.

---

## Results

| # | Question | Answer |
|---|---|---|
| **F1** | Does `add()` erase another client's restbus config on the same namespace? | **No.** |
| **F6** | Does a restbus write reach the bus on a frame an ECU transmits? | **Yes**, if the frame is already in that namespace's restbus. |

### F1 — refuted

Adding `VC_To_HMI` to `BCM-VehicleCAN`, a namespace where the BCM model runs its own
restbus (`RestbusConfig` + `SenderFilter(ecu_name="BCM")`), did **not** stop BCM's
`VSS_VehicleState` cycling:

```
"ecu_frame_still_cycling": true
counts: VSS_VehicleState.Vehicle_LowVoltageSystemState = 30   (unchanged, ~10/s)
```

The vCar kept running afterwards: BCM continued its scenario loop, VC kept reacting.
So the proto's *"Add removes any previous configuration"* is scoped **per client**,
not per namespace.

**Consequence:** `allow_add: true` remains the default. Loop B's design is unchanged.

### F6 — refuted, and the repo's recorded claim is wrong

`app/signals/service.py` states a restbus write to a frame an ECU transmits *"never
reaches the bus — the ECU's transmitter wins"*. Measured, on a frame with a real
cycle time, with **no `add()` call at all**:

```
wrote  VSS_VehicleState.Vehicle_LowVoltageSystemState = 5
observed at topology-VehicleCAN: {"5": 30}    # 30 of 30 frames
"sentinel_reached_bus": true
```

Independently corroborated by the VC container's own log — a real ECU consumed it:

```
VC: HMI output updated  ChimeId=0 TelltaleId=1 hazard=1 horn=1 lv_state=5
```

The original claim came from `{ECU}_DUMMY`, where `dummy_in` and `dummy_out` share one
8-byte frame and the stub echoes at CAN-simulator flood rate. That is a self-feeding
loop on the same bytes, not a general property of frames.

---

## Two findings that change how the bridge must behave

### 1. `update_signals` on a namespace with no restbus is silently ignored

The same write, to the same signal, on `topology-VehicleCAN`:

```
wrote  VC_To_HMI.TelltaleId = 2
observed: {"1": 30}          # VC's value, ours absent
"sentinel_reached_bus": false
```

No error. `UpdateRequest` carries no client id and there is nothing to catch — this is
the failure mode the design predicted from the proto, now observed.

**The rule:** `update_signals` only works where *something* has already added that frame
to that namespace's restbus. Writing to a namespace no ECU drives requires `add()` first.
A bridge that skips `add()` and targets a quiet namespace fails invisibly.

### 2. `add()` on a frame another ECU transmits creates a duplicate transmitter

This is the real hazard — not erasure, contention. After adding `VC_To_HMI` to
`BCM-VehicleCAN` while VC still transmits it from `VC-VehicleCAN`:

```
counts:  VC_To_HMI.TelltaleId = 60      # was 30 — two transmitters, both at 100 ms
values:  {"2": 31, "0": 31}             # ours and VC's, alternating
```

The receiver sees the signal flip between the two writers at 10 Hz each. Both writes
"reach the bus"; neither wins. A consumer reading edges would see a phantom 10 Hz
oscillation.

The same applies without `add()` when writing a signal an ECU actively drives: our
`LowVoltageSystemState = 5` held until BCM's scenario loop next wrote that signal, then
reverted (`{"5": 17, "2": 13}` across the transition). Last writer wins, and the ECU
writes every cycle.

**Consequence:** the `sender` warning in startup validation is the right response and
should stay. It is not "this will not work" — it demonstrably does — but "you are now
one of two writers on this frame, and the value will alternate". Two writers on one
actuator is a mapping error the operator must see.

---

## What the design should say

| Was | Is |
|---|---|
| F1 unverified; `allow_add` default might have to flip to `false` | F1 refuted for broker 1.24.5. Default stays `true`. |
| F6 unverified; add-then-update untested | Both update-only and add-then-update reach the bus. |
| `sender` warning justified by "add() may erase this namespace's config" | Justified by duplicate-transmitter contention instead. |
| — | New: `update_signals` on a namespace with no restbus is a silent no-op. Target an ECU namespace, or `add()` first. |

## Scope of the conclusion

One broker version (1.24.5), one topology, one channel, two ECUs. F1 was probed by
adding a frame the target namespace's owner does not itself transmit; a same-frame
`add()` collision was not tested separately, though §2 above shows what contention looks
like when it happens. Re-run the spike against a different broker version before relying
on these results there.

## Reproducing

```bash
cd bridge/vss-vcar
remotive topology generate -f instances/main.instance.yaml build   # CLI 0.15.1: 'generate', not 'build'
cd build/vss_hmi_vcar && docker compose up --build -d
cd ../../../ && uv run python scripts/spike_restbus.py
```

Requires an active RemotiveTopology subscription — the broker exits (0) on a licence
failure in every mode, including standalone, on tags 1.20 through 1.24.

---

# Addendum — F11: `set_target_values` does not drive a v2 provider

Date: 2026-08-01. Found while building `scripts/verify_bridge.py`, which failed
against a bridge that was in fact working correctly.

## What happens

`kuksa-client` 0.5.2 exposes two calls whose names imply they are two ends of one
channel. They are not:

| Call | Protocol | What it does |
|---|---|---|
| `set_target_values()` | v1 `Set` / `ACTUATOR_TARGET` | stores a value in the *Target Value* field |
| `subscribe_target_values()` | v2 `OpenProviderStream` | registers a **provider** and waits for *Actuation* requests |

v2 has no target-value perspective and v1 has no actuation, so the two never
meet. Measured against the live databroker, on a path with a registered provider:

```
set_target_values(True)        -> returns success
get_target_values()            -> reads back True
provider deliveries            -> []          # nothing at all
```

Same path, same provider, using the v2 RPC instead:

```
Actuate(signal_id=..., value=Value(bool=True))
provider deliveries            -> [{'Vehicle.Body.Lights.Hazard.IsSignaling': True}]
```

Confirmed on `Vehicle.Body.Lights.Hazard.IsSignaling`, a path no ECU provides, so
`ALREADY_EXISTS` could not confound the result.

## Consequences

**The bridge is correct and needs no change.** `subscribe_target_values()` is the
right way for a provider to receive commands; the KUKSA docs describe Target Value
as deprecated and v2 as actuation-only.

**Consumers must use `Actuate`, not `set_target_values`.** Anything commanding the
bridge — CPD, a dashboard, a test — has to issue a v2 `ActuateRequest`. A v1 target
write is silently ignored: it succeeds, it reads back, and nothing happens. There
is no error to catch, which makes it the same class of trap as F10.

Two details that cost time:

* The `Value` oneof field must match the catalog type exactly. `int32` into a
  `uint16` path is rejected with `INVALID_ARGUMENT: Wrong type for vss_path`.
* With no provider registered, `Actuate` fails `UNAVAILABLE: Provider for vss_id
  N does not exist`. Actuation is never buffered — it reaches a live provider or
  it is refused. That is a useful signal: it means "no bridge is running".

## Verifying

`scripts/verify_bridge.py` uses `Actuate` and is checked both ways: it passes with
the bridge running and fails, with a named reason, when it is stopped.
