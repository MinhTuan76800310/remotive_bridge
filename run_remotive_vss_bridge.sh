#!/usr/bin/env bash
#
# Run the RemotiveLabs ↔ KUKSA VSS bridge from the published image.
#
#   ./run_remotive_vss_bridge.sh                  # the example mapping in the image
#   ./run_remotive_vss_bridge.sh my-vehicle.yaml  # your own mapping
#   ./run_remotive_vss_bridge.sh -h               # options
#
# This file is meant to be handed out on its own. It needs Docker (or Podman)
# and nothing else — no checkout, no Python, no uv.
#
# It always pulls `latest` before starting, and always runs with `--rm`, so you
# get the current bridge and no container is left behind. Ctrl+C shuts the bridge
# down gracefully, which is what stops the restbus frames it started; killing it
# harder leaves those frames cycling on the bus.

set -euo pipefail

IMAGE="${BRIDGE_IMAGE:-ghcr.io/minhtuan76800310/remotive_bridge:latest}"

# The mapping baked into the image. Used when you pass no file of your own.
# It targets the `vss-vcar` rig at 127.0.0.1 — which is why the default network
# is `host`: inside a bridged container, 127.0.0.1 is the container itself.
EXAMPLE_IN_IMAGE="/usr/share/kx-vss-bridge/mapping.example.yaml"

NETWORK="${BRIDGE_NETWORK:-host}"
HEALTH_PORT="${BRIDGE_HEALTH_PORT:-8090}"
NAME="${BRIDGE_NAME:-kx-vss-bridge}"
PULL="${BRIDGE_PULL:-always}"

usage() {
  cat <<EOF
Usage: $(basename "$0") [MAPPING.yaml] [-- EXTRA ARGS...]

  MAPPING.yaml   Your mapping file. Mounted read-only at /config/mapping.yaml.
                 Omit it to use the example shipped in the image.

Environment:
  BRIDGE_IMAGE        image to run        (default: $IMAGE)
  BRIDGE_NETWORK      container network   (default: host)
  BRIDGE_HEALTH_PORT  published only when the network is NOT host (default: 8090)
  BRIDGE_NAME         container name      (default: kx-vss-bridge)
  BRIDGE_PULL         always | never      (default: always)
  BRIDGE_ENGINE       docker | podman     (default: whichever is installed)

Examples:
  $(basename "$0")
  $(basename "$0") ./my-vehicle.yaml
  $(basename "$0") ./my-vehicle.yaml -- --log-level DEBUG
  BRIDGE_NETWORK=vss_hmi_vcar_----control_network $(basename "$0") ./in-vcar.yaml

Once it is up:
  curl -s localhost:${HEALTH_PORT}/health | python3 -m json.tool
  curl -s localhost:${HEALTH_PORT}/stats  | python3 -m json.tool
EOF
}

# ── Container engine ─────────────────────────────────────────────────────────
if [[ -n "${BRIDGE_ENGINE:-}" ]]; then
  ENGINE="$BRIDGE_ENGINE"
elif command -v docker >/dev/null 2>&1; then
  ENGINE=docker
elif command -v podman >/dev/null 2>&1; then
  ENGINE=podman
else
  echo "error: neither docker nor podman is installed." >&2
  exit 127
fi
command -v "$ENGINE" >/dev/null 2>&1 || { echo "error: '$ENGINE' not found." >&2; exit 127; }

# ── Arguments ────────────────────────────────────────────────────────────────
MAPPING=""
EXTRA=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --)        shift; EXTRA=("$@"); break ;;
    -*)        echo "error: unknown option '$1' (pass bridge flags after --)" >&2
               echo >&2; usage >&2; exit 2 ;;
    *)         if [[ -n "$MAPPING" ]]; then
                 echo "error: only one mapping file; got '$MAPPING' and '$1'" >&2
                 exit 2
               fi
               MAPPING="$1"; shift ;;
  esac
done

MOUNT=()
if [[ -n "$MAPPING" ]]; then
  [[ -f "$MAPPING" ]] || { echo "error: no such file: $MAPPING" >&2; exit 2; }
  # -v needs an absolute path, and a relative one is silently treated as a
  # *named volume* by Docker rather than rejected.
  ABS="$(cd "$(dirname "$MAPPING")" && pwd)/$(basename "$MAPPING")"
  MOUNT=(-v "${ABS}:/config/mapping.yaml:ro")
  CONFIG_ARG="/config/mapping.yaml"
  echo "mapping:   $ABS"
else
  CONFIG_ARG="$EXAMPLE_IN_IMAGE"
  echo "mapping:   (example shipped in the image — pass a file to use your own)"
fi

# host networking publishes nothing; -p alongside it is an error on Podman and
# an ignored no-op on Docker.
PORTS=()
[[ "$NETWORK" == "host" ]] || PORTS=(-p "${HEALTH_PORT}:8090")

# ── Run ──────────────────────────────────────────────────────────────────────
if [[ "$PULL" != "never" ]]; then
  echo "pulling:   $IMAGE"
  "$ENGINE" pull "$IMAGE"
fi

# A container from a previous run that was killed rather than stopped still
# holds the name, and `run` would fail on the collision instead of starting.
"$ENGINE" rm -f "$NAME" >/dev/null 2>&1 || true

echo "image:     $IMAGE"
echo "network:   $NETWORK"
echo "health:    http://localhost:${HEALTH_PORT}/health"
echo "Ctrl+C to stop (graceful — this is what stops the restbus frames)."
echo

exec "$ENGINE" run --rm \
  --name "$NAME" \
  --network "$NETWORK" \
  "${PORTS[@]}" \
  "${MOUNT[@]}" \
  -e PYTHONUNBUFFERED=1 \
  "$IMAGE" \
  --config "$CONFIG_ARG" \
  "${EXTRA[@]}"
