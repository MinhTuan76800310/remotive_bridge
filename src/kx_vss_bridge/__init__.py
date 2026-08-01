"""kx_vss_bridge — a RemotiveLabs ↔ KUKSA VSS signal bridge.

The bridge is a client on both ends. It owns no state, embeds no databroker, and
publishes no signal of its own: it reads CAN signals from a RemotiveLabs broker
and writes them to VSS current values, and reads VSS actuation targets and writes
them to the RemotiveLabs restbus.

Design: docs/superpowers/specs/2026-08-01-vss-remotive-bridge-design.md
"""

from __future__ import annotations

__all__ = ["__version__"]

# Read from installed metadata rather than duplicated here, so the version
# cannot drift from pyproject.toml.
try:  # pragma: no cover - trivial import guard
    from importlib.metadata import PackageNotFoundError, version

    __version__ = version("kx-vss-bridge")
except PackageNotFoundError:  # pragma: no cover - source tree without install
    __version__ = "unknown"
