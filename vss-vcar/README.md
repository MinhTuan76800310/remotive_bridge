# vss-hmi-vcar

Two-ECU virtual car: **BCM** and **VC**, one CAN channel, two frames, six signals.

## Signals

| VSS path | frame | signal | sender |
|---|---|---|---|
| `Vehicle.Cabin.HMI.TelltaleId` | `VC_To_HMI` | `TelltaleId` | VC |
| `Vehicle.Cabin.HMI.ChimeId` | `VC_To_HMI` | `ChimeId` | VC |
| `Vehicle.Cabin.ChildPresence.IsDetected` | `VSS_VehicleState` | `Vehicle_Cabin_ChildPresence_IsDetected` | BCM |
| `Vehicle.LowVoltageSystemState` | `VSS_VehicleState` | `Vehicle_LowVoltageSystemState` | BCM |
| `Vehicle.Body.Lights.Hazard.IsSignaling` | `VSS_VehicleState` | `Vehicle_Body_Lights_Hazard_IsSignaling` | BCM |
| `Vehicle.Body.Horn.IsActive` | `VSS_VehicleState` | `Vehicle_Body_Horn_IsActive` | BCM |

Machine-readable copy: [platform/vss_signal_map.csv](platform/vss_signal_map.csv).
The VSS path of each signal is also carried as a `CM_` comment in
[platform/databases/VehicleState.dbc](platform/databases/VehicleState.dbc).

Both frames are on one CAN channel `VehicleCAN`, 500 kbit/s, 100 ms cycle time.

## Enums (DBC `VAL_` tables)

| signal | values |
|---|---|
| `Vehicle_LowVoltageSystemState` | 0 UNDEFINED, 1 LOCK, 2 OFF, 3 ACC, 4 ON, 5 START |
| `TelltaleId` | 0 NONE, 1 CHILD_PRESENCE, 2 HAZARD |
| `ChimeId` | 0 NONE, 1 CHILD_PRESENCE_ALERT |

## Behaviour

```
BCM --VSS_VehicleState--> VC        child presence, ignition state, hazard, horn
VC  --VC_To_HMI---------> BCM       TelltaleId, ChimeId
```

- **VC** maps vehicle state to HMI output. Decision table in
  [models/vc/python/vc/control.py](models/vc/python/vc/control.py):
  child detected while parked (`LOCK`/`OFF`) → `CHILD_PRESENCE` + `CHILD_PRESENCE_ALERT`;
  child detected while driving → telltale only; otherwise hazard is reflected as a telltale.
- **BCM** provides the state frame and escalates the alert: on `ChimeId == CHILD_PRESENCE_ALERT`
  it sets `Hazard.IsSignaling` and `Horn.IsActive`, and clears them when the chime clears.
- BCM runs a built-in scenario loop (driving → parked → child left behind → child removed,
  30 s per cycle) so the car moves on its own. Set `BCM_SCENARIO=0` to freeze it and drive
  the signals externally instead.

## Run

```bash
cd vcars/vss-hmi-vcar
make gen     # remotive topology build -> build/vss_hmi_vcar/
make up      # docker compose up --build -d
make logs
make down
```

`make unit` runs the VC decision-table self-check without a broker or Docker.

Requires the RemotiveLabs CLI (`remotive`) and Docker. Broker tag pinned to `1.23.0`.
