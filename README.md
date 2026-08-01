# kx-vss-bridge

A standalone service that maps RemotiveLabs CAN signals to Eclipse KUKSA VSS
signals, 1-to-1, in both directions.

The bridge is a client on both ends. It owns no state, embeds no databroker, and
publishes no signal of its own.

```
┌─ vCar (RemotiveLabs) ────────┐
│  ECUs ──── topology-broker   │
└───────────────┬──────────────┘
                │ gRPC 50051
         ┌──────┴───────┐
         │  vss-bridge  │ ←── mapping.yaml
         └──────┬───────┘
                │ gRPC 55555
┌───────────────┴──────────────┐
│  KUKSA databroker            │
│  VSS 6.0 + overlays          │
└──────────────────────────────┘
```

- **Remotive → VSS** writes **current values** (what the vehicle reports).
- **VSS → Remotive** reads **actuation targets** (what a function commands) and
  writes them to the restbus.

Design: [`docs/superpowers/specs/2026-08-01-vss-remotive-bridge-design.md`](../docs/superpowers/specs/2026-08-01-vss-remotive-bridge-design.md)

## Status

Under implementation. The full operator runbook — mapping reference, transforms,
networking, `/health` and `/stats` — lands with the documentation task.

## Development

```bash
uv sync --dev
uv run pytest -q
```
