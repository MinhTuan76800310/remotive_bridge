"""Tests for runtime state and the bounded latest-value buffers.

The buffer is the piece that lets one peer fail without stalling the other. Its
contract is narrow and worth stating: a producer never blocks, a slow or absent
consumer never causes unbounded growth, and an acknowledgement never discards a
value the producer wrote *after* the consumer took its snapshot. That last one is
the race these tests spend most of their effort on.
"""

from __future__ import annotations

import asyncio

import pytest

from kx_vss_bridge.state import BridgeState, Direction, Peer

TO_VSS = Direction.TO_VSS
TO_CAN = Direction.TO_CAN


# ── initial state ────────────────────────────────────────────────────────────


async def test_starts_disconnected_and_degraded():
    snapshot = await BridgeState().snapshot()
    assert snapshot["status"] == "degraded"
    assert snapshot["remotive"]["connected"] is False
    assert snapshot["kuksa"]["connected"] is False


async def test_uptime_is_reported():
    assert "uptime_s" in await BridgeState().snapshot()


# ── peer connection and phase ────────────────────────────────────────────────


async def test_peers_are_tracked_independently():
    state = BridgeState()
    await state.set_peer(Peer.REMOTIVE, connected=True, phase="streaming")
    snapshot = await state.snapshot()
    assert snapshot["remotive"]["connected"] is True
    assert snapshot["remotive"]["phase"] == "streaming"
    assert snapshot["kuksa"]["connected"] is False


async def test_status_is_ok_only_when_both_peers_are_up():
    state = BridgeState()
    await state.set_peer(Peer.REMOTIVE, connected=True, phase="streaming")
    assert (await state.snapshot())["status"] == "degraded"
    await state.set_peer(Peer.KUKSA, connected=True, phase="streaming")
    assert (await state.snapshot())["status"] == "ok"


async def test_a_skipped_mapping_keeps_status_degraded():
    """Connected but silently dropping a mapping is not 'ok'."""
    state = BridgeState()
    await state.set_peer(Peer.REMOTIVE, connected=True, phase="streaming")
    await state.set_peer(Peer.KUKSA, connected=True, phase="streaming")
    await state.replace_validation(
        skipped=[{"entry": "F.S", "reason": "not in vehicle"}], warnings=[]
    )
    assert (await state.snapshot())["status"] == "degraded"


# ── counters ─────────────────────────────────────────────────────────────────


async def test_reconnects_and_last_error_are_recorded():
    state = BridgeState()
    await state.record_reconnect(Peer.KUKSA, RuntimeError("UNAVAILABLE"))
    await state.record_reconnect(Peer.KUKSA, RuntimeError("UNAVAILABLE"))
    kuksa = (await state.snapshot())["kuksa"]
    assert kuksa["reconnects"] == 2
    assert "UNAVAILABLE" in kuksa["last_error"]


async def test_batch_and_write_counters_accumulate():
    state = BridgeState()
    await state.record_can_batch(4)
    await state.record_can_batch(2)
    await state.record_write(TO_VSS, 3)
    snapshot = await state.snapshot()
    assert snapshot["remotive"]["batches"] == 2
    assert snapshot["remotive"]["signals"] == 6
    assert snapshot["mapping"]["to_vss_writes"] == 3


async def test_drops_are_counted_and_the_reason_is_kept():
    state = BridgeState()
    await state.record_drop(TO_VSS, "F.S", "outside configured range")
    snapshot = await state.snapshot()
    assert snapshot["mapping"]["to_vss_drops"] == 1
    assert snapshot["drops"][0]["reason"] == "outside configured range"


async def test_repeated_drops_are_deduplicated_but_counted():
    """A signal failing at 100 Hz must not grow /stats without bound."""
    state = BridgeState()
    for _ in range(500):
        await state.record_drop(TO_VSS, "F.S", "outside configured range")
    snapshot = await state.snapshot()
    assert snapshot["mapping"]["to_vss_drops"] == 500
    assert len(snapshot["drops"]) == 1
    assert snapshot["drops"][0]["count"] == 500


async def test_distinct_drop_reasons_are_kept_separately():
    state = BridgeState()
    await state.record_drop(TO_VSS, "F.A", "range")
    await state.record_drop(TO_VSS, "F.B", "unmapped enum")
    assert len({d["entry"] for d in (await state.snapshot())["drops"]}) == 2


async def test_drop_list_is_bounded():
    """A pathological mapping must not turn /stats into a memory leak."""
    state = BridgeState()
    for index in range(1000):
        await state.record_drop(TO_VSS, f"F.S{index}", "range")
    snapshot = await state.snapshot()
    assert len(snapshot["drops"]) <= 50
    assert snapshot["mapping"]["to_vss_drops"] == 1000


# ── the latest-value buffer ──────────────────────────────────────────────────


async def test_put_then_snapshot_returns_the_value():
    state = BridgeState()
    await state.put_latest(TO_VSS, "Vehicle.Speed", 42.0)
    version, pending = await state.pending_snapshot(TO_VSS)
    assert pending == {"Vehicle.Speed": 42.0}
    assert version > 0


async def test_newer_value_replaces_older_for_the_same_path():
    """Bounded: while a peer is down, one slot per path, not a queue."""
    state = BridgeState()
    for speed in (1.0, 2.0, 3.0):
        await state.put_latest(TO_VSS, "Vehicle.Speed", speed)
    _, pending = await state.pending_snapshot(TO_VSS)
    assert pending == {"Vehicle.Speed": 3.0}


async def test_acknowledge_clears_the_snapshot():
    state = BridgeState()
    await state.put_latest(TO_VSS, "Vehicle.Speed", 42.0)
    version, _ = await state.pending_snapshot(TO_VSS)
    await state.acknowledge(TO_VSS, version)
    _, pending = await state.pending_snapshot(TO_VSS)
    assert pending == {}


async def test_acknowledge_does_not_discard_a_value_written_after_the_snapshot():
    """The race the version number exists for.

    Consumer takes a snapshot, the write is slow, the producer stores a fresher
    value meanwhile. Acknowledging the old version must not drop the new value —
    that would strand the peer on a stale reading until the signal next changes.
    """
    state = BridgeState()
    await state.put_latest(TO_VSS, "Vehicle.Speed", 1.0)
    version, taken = await state.pending_snapshot(TO_VSS)
    assert taken == {"Vehicle.Speed": 1.0}

    await state.put_latest(TO_VSS, "Vehicle.Speed", 2.0)  # arrives mid-write
    await state.acknowledge(TO_VSS, version)

    _, pending = await state.pending_snapshot(TO_VSS)
    assert pending == {"Vehicle.Speed": 2.0}


async def test_unacknowledged_values_survive_for_a_later_flush():
    """A failed write leaves the buffer pending, so reconnect flushes it."""
    state = BridgeState()
    await state.put_latest(TO_VSS, "Vehicle.Speed", 42.0)
    await state.pending_snapshot(TO_VSS)  # write fails; no acknowledge
    _, pending = await state.pending_snapshot(TO_VSS)
    assert pending == {"Vehicle.Speed": 42.0}


async def test_the_two_directions_have_independent_buffers():
    state = BridgeState()
    await state.put_latest(TO_VSS, "Vehicle.Speed", 42.0)
    await state.put_latest(TO_CAN, "Vehicle.Body.Horn.IsActive", True)
    _, to_vss = await state.pending_snapshot(TO_VSS)
    _, to_can = await state.pending_snapshot(TO_CAN)
    assert to_vss == {"Vehicle.Speed": 42.0}
    assert to_can == {"Vehicle.Body.Horn.IsActive": True}


async def test_acknowledging_one_direction_leaves_the_other_alone():
    state = BridgeState()
    await state.put_latest(TO_VSS, "Vehicle.Speed", 42.0)
    await state.put_latest(TO_CAN, "Vehicle.Body.Horn.IsActive", True)
    version, _ = await state.pending_snapshot(TO_VSS)
    await state.acknowledge(TO_VSS, version)
    _, to_can = await state.pending_snapshot(TO_CAN)
    assert to_can == {"Vehicle.Body.Horn.IsActive": True}


# ── waiting ──────────────────────────────────────────────────────────────────


async def test_wait_returns_immediately_when_something_is_pending():
    state = BridgeState()
    await state.put_latest(TO_VSS, "Vehicle.Speed", 42.0)
    await asyncio.wait_for(state.wait_for_pending(TO_VSS), timeout=1.0)


async def test_wait_blocks_until_a_value_arrives():
    state = BridgeState()
    waiter = asyncio.create_task(state.wait_for_pending(TO_VSS))
    await asyncio.sleep(0)
    assert not waiter.done()

    await state.put_latest(TO_VSS, "Vehicle.Speed", 42.0)
    await asyncio.wait_for(waiter, timeout=1.0)


async def test_wait_is_not_woken_by_the_other_direction():
    state = BridgeState()
    waiter = asyncio.create_task(state.wait_for_pending(TO_VSS))
    await asyncio.sleep(0)
    await state.put_latest(TO_CAN, "Vehicle.Body.Horn.IsActive", True)
    await asyncio.sleep(0.01)
    assert not waiter.done()
    waiter.cancel()


async def test_wait_can_be_cancelled():
    state = BridgeState()
    waiter = asyncio.create_task(state.wait_for_pending(TO_VSS))
    await asyncio.sleep(0)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter


# ── concurrency ──────────────────────────────────────────────────────────────


async def test_concurrent_counter_updates_are_not_lost():
    state = BridgeState()
    await asyncio.gather(*(state.record_can_batch(1) for _ in range(200)))
    assert (await state.snapshot())["remotive"]["batches"] == 200


async def test_concurrent_writes_to_one_path_leave_a_consistent_value():
    state = BridgeState()
    await asyncio.gather(*(state.put_latest(TO_VSS, "Vehicle.Speed", n) for n in range(100)))
    _, pending = await state.pending_snapshot(TO_VSS)
    assert pending["Vehicle.Speed"] in range(100)


async def test_producer_and_consumer_lose_no_final_value():
    """Interleave a producer and a consumer; the last value must land."""
    state = BridgeState()
    delivered: dict[str, object] = {}

    async def produce() -> None:
        for n in range(50):
            await state.put_latest(TO_VSS, "Vehicle.Speed", n)
            await asyncio.sleep(0)

    async def consume() -> None:
        for _ in range(50):
            version, pending = await state.pending_snapshot(TO_VSS)
            delivered.update(pending)
            await state.acknowledge(TO_VSS, version)
            await asyncio.sleep(0)

    await asyncio.gather(produce(), consume())
    _, leftover = await state.pending_snapshot(TO_VSS)
    delivered.update(leftover)
    assert delivered["Vehicle.Speed"] == 49


# ── validation results ───────────────────────────────────────────────────────


async def test_validation_results_replace_rather_than_accumulate():
    """Re-validating on every reconnect must not grow the list forever."""
    state = BridgeState()
    for _ in range(5):
        await state.replace_validation(
            skipped=[{"entry": "F.S", "reason": "not in vehicle"}],
            warnings=[{"frame": "F", "note": "no cycle time"}],
        )
    snapshot = await state.snapshot()
    assert len(snapshot["mapping"]["skipped"]) == 1
    assert len(snapshot["warnings"]) == 1


async def test_active_mapping_counts_are_reported():
    state = BridgeState()
    await state.replace_validation(to_vss=42, to_can=7, skipped=[], warnings=[])
    mapping = (await state.snapshot())["mapping"]
    assert mapping["to_vss"] == 42
    assert mapping["to_can"] == 7


# ── snapshot hygiene ─────────────────────────────────────────────────────────


async def test_snapshot_is_json_serialisable():
    import json

    state = BridgeState()
    await state.set_peer(Peer.REMOTIVE, connected=True, phase="seeding")
    await state.record_drop(TO_VSS, "F.S", "range")
    await state.record_reconnect(Peer.KUKSA, RuntimeError("boom"))
    json.dumps(await state.snapshot())


async def test_snapshot_reports_ages_not_wall_clock():
    state = BridgeState()
    await state.record_can_batch(1)
    remotive = (await state.snapshot())["remotive"]
    assert remotive["last_batch_s_ago"] is not None
    assert remotive["last_batch_s_ago"] >= 0
    assert "last_batch_at" not in remotive


async def test_age_is_null_before_anything_happens():
    assert (await BridgeState().snapshot())["remotive"]["last_batch_s_ago"] is None


async def test_snapshot_carries_no_buffered_signal_values():
    """/stats is diagnostics, not a data plane. Buffered values stay internal."""
    state = BridgeState()
    await state.put_latest(TO_VSS, "Vehicle.Speed", 99.5)
    assert "99.5" not in str(await state.snapshot())


async def test_snapshot_is_a_copy():
    state = BridgeState()
    snapshot = await state.snapshot()
    snapshot["remotive"]["batches"] = 9999
    assert (await state.snapshot())["remotive"]["batches"] == 0
