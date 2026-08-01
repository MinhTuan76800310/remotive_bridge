"""Runtime state: observability counters and the two hand-off buffers.

The buffers are the reason a dead KUKSA does not stop the bridge reading CAN, and
a dead broker does not stop it consuming VSS targets. Each direction has one:

    producer --put_latest()-->  [ one slot per path ]  --pending_snapshot()--> consumer
                                                       <--acknowledge(version)--

Three properties, in order of how easy they are to get wrong:

1. **Bounded.** One slot per path, not a queue. While a peer is down a fast
   signal replaces its own previous value instead of accumulating. Memory is
   bounded by the mapping size, which is known at startup.
2. **Acknowledge is version-scoped.** A consumer takes a snapshot, writes it
   slowly, and acknowledges. If the producer stored a fresher value in between,
   acknowledging the *old* version must not discard it — otherwise the peer sits
   on a stale reading until that signal happens to change again. Each slot
   remembers the version at which it was written, and acknowledgement only
   clears slots not touched since.
3. **Nothing blocks the producer.** `put_latest` takes the lock, writes one
   entry, notifies, and returns.

State holds diagnostics and these buffers. It never holds SDK clients, streams,
or anything derived from the KUKSA token — `snapshot()` is served over HTTP.
"""

from __future__ import annotations

import asyncio
import enum
import time
from typing import Any

__all__ = ["BridgeState", "Direction", "Peer"]

# /stats is a diagnostic surface, not a log. A mapping that fails on every frame
# at 100 Hz must not grow it without bound, so distinct entries are capped and
# repeats collapse into a count.
_MAX_TRACKED_DROPS = 50


class Peer(enum.Enum):
    REMOTIVE = "remotive"
    KUKSA = "kuksa"


class Direction(enum.Enum):
    TO_VSS = "to_vss"
    TO_CAN = "to_can"


class _PeerState:
    """Connection health for one peer."""

    def __init__(self) -> None:
        self.connected = False
        self.phase = "starting"
        self.reconnects = 0
        self.errors = 0
        self.last_error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "connected": self.connected,
            "phase": self.phase,
            "reconnects": self.reconnects,
            "errors": self.errors,
            "last_error": self.last_error,
        }


class _Buffer:
    """A bounded latest-value buffer for one direction.

    `_version` increments on every write and is stored per slot, which is what
    makes acknowledgement safe against a concurrent producer.
    """

    def __init__(self) -> None:
        self._values: dict[str, Any] = {}
        self._versions: dict[str, int] = {}
        self._version = 0
        self.writes = 0
        self.drops = 0

    def put(self, path: str, value: Any) -> int:
        self._version += 1
        self._values[path] = value
        self._versions[path] = self._version
        return self._version

    def snapshot(self) -> tuple[int, dict[str, Any]]:
        return self._version, dict(self._values)

    def acknowledge(self, version: int) -> None:
        for path, written_at in list(self._versions.items()):
            if written_at <= version:
                del self._versions[path]
                self._values.pop(path, None)

    @property
    def pending(self) -> bool:
        return bool(self._values)


class BridgeState:
    """Shared, lock-guarded state. Every method is a coroutine on purpose.

    A single lock is enough: every operation is a handful of dict writes, and
    two locks would invite an ordering bug for no measurable gain.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        # Condition shares the lock, so "write then notify" is one atomic step
        # and a waiter cannot miss a value written between its check and wait.
        self._changed = asyncio.Condition(self._lock)

        self._started_at = time.monotonic()
        self._peers = {peer: _PeerState() for peer in Peer}
        self._buffers = {direction: _Buffer() for direction in Direction}

        self._batches = 0
        self._signals = 0
        self._last_batch_at: float | None = None

        # Keyed by (direction, entry, reason) so a repeating failure collapses.
        self._drops: dict[tuple[str, str, str], int] = {}

        self._active = {Direction.TO_VSS: 0, Direction.TO_CAN: 0}
        self._skipped: tuple[dict[str, Any], ...] = ()
        self._warnings: tuple[dict[str, Any], ...] = ()

    # ── peers ────────────────────────────────────────────────────────────────

    async def set_peer(self, peer: Peer, *, connected: bool, phase: str) -> None:
        async with self._lock:
            state = self._peers[peer]
            state.connected = connected
            state.phase = phase

    async def record_reconnect(self, peer: Peer, error: BaseException | None = None) -> None:
        async with self._lock:
            state = self._peers[peer]
            state.connected = False
            state.reconnects += 1
            if error is not None:
                state.errors += 1
                state.last_error = f"{type(error).__name__}: {error}"

    # ── counters ─────────────────────────────────────────────────────────────

    async def record_can_batch(self, size: int) -> None:
        async with self._lock:
            self._batches += 1
            self._signals += size
            self._last_batch_at = time.monotonic()

    async def record_write(self, direction: Direction, count: int) -> None:
        async with self._lock:
            self._buffers[direction].writes += count

    async def record_drop(self, direction: Direction, entry: str, reason: str) -> None:
        async with self._lock:
            self._buffers[direction].drops += 1
            key = (direction.value, entry, reason)
            if key in self._drops:
                self._drops[key] += 1
            elif len(self._drops) < _MAX_TRACKED_DROPS:
                self._drops[key] = 1
            # Beyond the cap the total still counts; only the per-entry detail
            # is dropped. A truncated list is better than an unbounded one.

    # ── the buffers ──────────────────────────────────────────────────────────

    async def put_latest(self, direction: Direction, path: str, value: Any) -> int:
        """Store the newest value for `path`, replacing any unsent one."""
        async with self._changed:
            version = self._buffers[direction].put(path, value)
            self._changed.notify_all()
            return version

    async def pending_snapshot(self, direction: Direction) -> tuple[int, dict[str, Any]]:
        """Everything waiting to be sent, with the version it was taken at."""
        async with self._lock:
            return self._buffers[direction].snapshot()

    async def acknowledge(self, direction: Direction, version: int) -> None:
        """Clear what was successfully sent, keeping anything newer."""
        async with self._lock:
            self._buffers[direction].acknowledge(version)

    async def wait_for_pending(self, direction: Direction) -> None:
        """Block until this direction has something to send."""
        async with self._changed:
            await self._changed.wait_for(lambda: self._buffers[direction].pending)

    # ── validation ───────────────────────────────────────────────────────────

    async def replace_validation(
        self,
        *,
        skipped: list[dict[str, Any]],
        warnings: list[dict[str, Any]],
        to_vss: int | None = None,
        to_can: int | None = None,
    ) -> None:
        """Replace, never append — validation re-runs on every reconnect."""
        async with self._lock:
            self._skipped = tuple(skipped)
            self._warnings = tuple(warnings)
            if to_vss is not None:
                self._active[Direction.TO_VSS] = to_vss
            if to_can is not None:
                self._active[Direction.TO_CAN] = to_can

    # ── snapshot ─────────────────────────────────────────────────────────────

    async def snapshot(self) -> dict[str, Any]:
        """A JSON-serialisable copy for /health and /stats.

        Times are rendered as ages. An absolute clock reading would be useless
        to a reader in another timezone and meaningless across a restart.
        """
        async with self._lock:
            now = time.monotonic()
            both_up = all(peer.connected for peer in self._peers.values())
            to_vss = self._buffers[Direction.TO_VSS]
            to_can = self._buffers[Direction.TO_CAN]

            return {
                "status": "ok" if both_up and not self._skipped else "degraded",
                "uptime_s": round(now - self._started_at, 1),
                "remotive": {
                    **self._peers[Peer.REMOTIVE].as_dict(),
                    "batches": self._batches,
                    "signals": self._signals,
                    "last_batch_s_ago": (
                        None if self._last_batch_at is None
                        else round(now - self._last_batch_at, 3)
                    ),
                },
                "kuksa": self._peers[Peer.KUKSA].as_dict(),
                "mapping": {
                    "to_vss": self._active[Direction.TO_VSS],
                    "to_can": self._active[Direction.TO_CAN],
                    "to_vss_writes": to_vss.writes,
                    "to_can_writes": to_can.writes,
                    "to_vss_drops": to_vss.drops,
                    "to_can_drops": to_can.drops,
                    "skipped": [dict(item) for item in self._skipped],
                },
                "drops": [
                    {"direction": direction, "entry": entry, "reason": reason, "count": count}
                    for (direction, entry, reason), count in self._drops.items()
                ],
                "warnings": [dict(item) for item in self._warnings],
            }
