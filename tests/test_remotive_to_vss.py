"""Tests for Loop A — Remotive to VSS.

The loop is two independent workers, not one. That is the whole point: a KUKSA
outage must not stop the bridge reading CAN, and a broker outage must not stop it
flushing what it already has. They meet at a bounded latest-value buffer.

    reader  --put_latest()-->  [ one slot per path ]  --pending_snapshot()--> writer
                                                      <--acknowledge(version)--

So the tests fall into three groups: what the reader does with the broker, what
the writer does with KUKSA, and what happens at the seam when one side dies.
"""

from __future__ import annotations

import asyncio

import pytest

from kx_vss_bridge.config import load_config_text
from kx_vss_bridge.remotive_to_vss import (
    run_kuksa_current_writer,
    run_remotive_reader,
)
from kx_vss_bridge.state import BridgeState, Direction, Peer
from tests.fakes import (
    FakeBrokerClient,
    FakeVSSClient,
    RecordingSleep,
    make_frame_info,
    signal,
)

NS = "BCM-VehicleCAN"
FRAME = "VSS_VehicleState"
LV = f"{FRAME}.Vehicle_LowVoltageSystemState"
CHILD = f"{FRAME}.Vehicle_Cabin_ChildPresence_IsDetected"

LV_PATH = "Vehicle.LowVoltageSystemState"
CHILD_PATH = "Vehicle.Cabin.ChildPresence.IsDetected"

CONFIG = f"""
remotive: {{url: http://broker:50051}}
kuksa: {{host: kuksa, port: 55557}}
options: {{seed_seconds: 0.05, retry_delay: 7}}
to_vss:
  - can: {{namespace: {NS}, signal: {LV}}}
    vss: {LV_PATH}
    type: string
    transform:
      op: enum
      map: {{0: UNDEFINED, 1: LOCK, 2: "OFF", 3: ACC, 4: "ON", 5: START}}
  - can: {{namespace: {NS}, signal: {CHILD}}}
    vss: {CHILD_PATH}
    type: string
    transform: {{op: enum, map: {{0: NOT_DETECTED, 1: DETECTED}}}}
to_can:
  - vss: Vehicle.Body.Horn.IsActive
    can: {{namespace: {NS}, signal: {FRAME}.Vehicle_Body_Horn_IsActive}}
    type: boolean
    transform: {{op: threshold, gt: 0}}
"""

KUKSA_PATHS = [LV_PATH, CHILD_PATH, "Vehicle.Body.Horn.IsActive"]


def _config():
    return load_config_text(CONFIG).config


def _broker(**kwargs) -> FakeBrokerClient:
    kwargs.setdefault(
        "frame_infos",
        {
            NS: [
                make_frame_info(
                    FRAME,
                    NS,
                    [LV, CHILD, f"{FRAME}.Vehicle_Body_Horn_IsActive"],
                )
            ]
        },
    )
    return FakeBrokerClient(**kwargs)


async def _run_briefly(coro, timeout: float = 0.4) -> None:
    """Run a forever-loop until it is cancelled or the timeout expires.

    The workers are designed never to return, so every test that exercises one
    pays this timeout in real time. It is kept small deliberately: `seed_seconds`
    is 0.05 in these fixtures, so 0.4 s is ample for seed plus a few batches, and
    the difference across the file is half a minute of waiting.
    """
    task = asyncio.create_task(coro)
    try:
        await asyncio.wait_for(task, timeout=timeout)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


# ── the reader ───────────────────────────────────────────────────────────────


async def test_reader_seeds_then_streams():
    """Two subscriptions per connection: on_change=False, then True."""
    broker = _broker(
        seed_batches=[[signal(NS, LV, 4)]],
        batches=[[signal(NS, LV, 2)]],
        hang_after_batches=True,
    )
    state = BridgeState()
    await _run_briefly(
        run_remotive_reader(
            _config(), state, broker_factory=lambda: broker,
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=RecordingSleep(1),
        )
    )
    assert [c["on_change"] for c in broker.subscribe_calls][:2] == [False, True]


async def test_reader_subscribes_to_every_active_signal_in_one_call():
    broker = _broker(hang_after_batches=True)
    await _run_briefly(
        run_remotive_reader(
            _config(), BridgeState(), broker_factory=lambda: broker,
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=RecordingSleep(1),
        )
    )
    groups = broker.subscribe_calls[0]["signals"]
    assert len(groups) == 1
    assert sorted(groups[0][1]) == sorted([LV, CHILD])


async def test_seed_values_reach_the_buffer():
    broker = _broker(seed_batches=[[signal(NS, LV, 4)]], hang_after_batches=True)
    state = BridgeState()
    await _run_briefly(
        run_remotive_reader(
            _config(), state, broker_factory=lambda: broker,
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=RecordingSleep(1),
        )
    )
    _, pending = await state.pending_snapshot(Direction.TO_VSS)
    assert pending[LV_PATH] == "ON"


async def test_a_quiet_broker_does_not_hang_the_seed_phase():
    """The deadline must break the stream, not only be checked inside it.

    A stream that yields nothing would otherwise block in `async for` forever,
    and the bridge would never reach the streaming phase.
    """
    broker = _broker(seed_batches=[], hang_after_batches=True)
    state = BridgeState()
    await _run_briefly(
        run_remotive_reader(
            _config(), state, broker_factory=lambda: broker,
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=RecordingSleep(1),
        ),
        timeout=0.6,
    )
    assert len(broker.subscribe_calls) >= 2  # it got past seeding


async def test_streamed_values_are_transformed_before_buffering():
    broker = _broker(batches=[[signal(NS, LV, 2), signal(NS, CHILD, 1)]], hang_after_batches=True)
    state = BridgeState()
    await _run_briefly(
        run_remotive_reader(
            _config(), state, broker_factory=lambda: broker,
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=RecordingSleep(1),
        )
    )
    _, pending = await state.pending_snapshot(Direction.TO_VSS)
    assert pending == {LV_PATH: "OFF", CHILD_PATH: "DETECTED"}


async def test_an_unmapped_signal_is_ignored():
    broker = _broker(
        batches=[[signal(NS, f"{FRAME}.Unmapped", 1), signal(NS, LV, 2)]],
        hang_after_batches=True,
    )
    state = BridgeState()
    await _run_briefly(
        run_remotive_reader(
            _config(), state, broker_factory=lambda: broker,
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=RecordingSleep(1),
        )
    )
    _, pending = await state.pending_snapshot(Direction.TO_VSS)
    assert pending == {LV_PATH: "OFF"}


async def test_a_bad_value_drops_only_itself():
    """99 is not in the enum map; the sibling in the same batch must survive.

    The count is >= 1 rather than == 1 because the reader reconnects and replays
    the batch; what matters is that the bad value never reaches the buffer and
    the good one always does.
    """
    broker = _broker(
        batches=[[signal(NS, LV, 99), signal(NS, CHILD, 1)]], hang_after_batches=True
    )
    state = BridgeState()
    await _run_briefly(
        run_remotive_reader(
            _config(), state, broker_factory=lambda: broker,
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=RecordingSleep(1),
        )
    )
    _, pending = await state.pending_snapshot(Direction.TO_VSS)
    assert pending == {CHILD_PATH: "DETECTED"}
    assert (await state.snapshot())["mapping"]["to_vss_drops"] >= 1


async def test_batches_are_counted():
    broker = _broker(
        batches=[[signal(NS, LV, 2)], [signal(NS, LV, 4)]], hang_after_batches=True
    )
    state = BridgeState()
    await _run_briefly(
        run_remotive_reader(
            _config(), state, broker_factory=lambda: broker,
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=RecordingSleep(1),
        )
    )
    assert (await state.snapshot())["remotive"]["batches"] >= 2


async def test_validation_runs_on_every_connection():
    """A vehicle can change under a reconnect; cached validation would lie."""
    broker = _broker(batches=[[signal(NS, LV, 2)]])
    calls = 0
    original = broker.list_frame_infos

    async def spy(*namespaces):
        nonlocal calls
        calls += 1
        return await original(*namespaces)

    broker.list_frame_infos = spy  # type: ignore[method-assign]
    await _run_briefly(
        run_remotive_reader(
            _config(), BridgeState(), broker_factory=lambda: broker,
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=RecordingSleep(3),
        )
    )
    assert calls >= 2  # reconnected at least once


# ── reconnection ─────────────────────────────────────────────────────────────


async def test_a_clean_stream_end_backs_off_instead_of_busy_looping():
    """No exception is raised when a stream simply ends. Retry must still wait."""
    broker = _broker(batches=[[signal(NS, LV, 2)]])
    sleeper = RecordingSleep(stop_after=3)
    await _run_briefly(
        run_remotive_reader(
            _config(), BridgeState(), broker_factory=lambda: broker,
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=sleeper,
        )
    )
    assert sleeper.delays and all(d == 7 for d in sleeper.delays)


async def test_a_connection_failure_is_retried_forever():
    sleeper = RecordingSleep(stop_after=3)
    await _run_briefly(
        run_remotive_reader(
            _config(), BridgeState(),
            broker_factory=lambda: FakeBrokerClient(connect_error=ConnectionError("down")),
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=sleeper,
        )
    )
    assert len(sleeper.delays) == 3


async def test_a_mid_stream_error_is_retried_and_recorded():
    state = BridgeState()
    sleeper = RecordingSleep(stop_after=2)
    await _run_briefly(
        run_remotive_reader(
            _config(), state,
            broker_factory=lambda: _broker(
                batches=[[signal(NS, LV, 2)]], stream_error=RuntimeError("stream died")
            ),
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=sleeper,
        )
    )
    snapshot = await state.snapshot()
    assert snapshot["remotive"]["reconnects"] >= 1
    assert "stream died" in snapshot["remotive"]["last_error"]


async def test_the_reader_survives_kuksa_being_unreachable():
    """Validation needs KUKSA. If it is down, keep retrying, do not crash."""
    sleeper = RecordingSleep(stop_after=2)
    await _run_briefly(
        run_remotive_reader(
            _config(), BridgeState(), broker_factory=lambda: _broker(),
            kuksa_factory=lambda: FakeVSSClient(connect_error=ConnectionError("no kuksa")),
            sleep=sleeper,
        )
    )
    assert len(sleeper.delays) == 2


async def test_cancellation_propagates():
    async def never(_):
        await asyncio.Event().wait()

    task = asyncio.create_task(
        run_remotive_reader(
            _config(), BridgeState(),
            broker_factory=lambda: _broker(hang_after_batches=True),
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=never,
        )
    )
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


# ── the writer ───────────────────────────────────────────────────────────────


async def test_the_writer_flushes_what_is_buffered():
    state = BridgeState()
    await state.put_latest(Direction.TO_VSS, LV_PATH, "OFF")
    kuksa = FakeVSSClient(KUKSA_PATHS)
    await _run_briefly(
        run_kuksa_current_writer(
            _config(), state, kuksa_factory=lambda: kuksa, sleep=RecordingSleep(1)
        )
    )
    assert len(kuksa.set_calls) == 1
    assert kuksa.set_calls[0][0].entry.path == LV_PATH


async def test_one_snapshot_becomes_one_set_call():
    """Eight signals changing together must not become eight round-trips."""
    state = BridgeState()
    await state.put_latest(Direction.TO_VSS, LV_PATH, "OFF")
    await state.put_latest(Direction.TO_VSS, CHILD_PATH, "DETECTED")
    kuksa = FakeVSSClient(KUKSA_PATHS)
    await _run_briefly(
        run_kuksa_current_writer(
            _config(), state, kuksa_factory=lambda: kuksa, sleep=RecordingSleep(1)
        )
    )
    assert len(kuksa.set_calls) == 1
    assert len(kuksa.set_calls[0]) == 2


async def test_writes_carry_an_explicit_data_type():
    """Without metadata, kuksa-client fetches the type before every write."""
    from kuksa_client.grpc import DataType

    state = BridgeState()
    await state.put_latest(Direction.TO_VSS, LV_PATH, "OFF")
    kuksa = FakeVSSClient(KUKSA_PATHS)
    await _run_briefly(
        run_kuksa_current_writer(
            _config(), state, kuksa_factory=lambda: kuksa, sleep=RecordingSleep(1)
        )
    )
    entry = kuksa.set_calls[0][0].entry
    assert entry.metadata is not None
    assert entry.metadata.data_type is DataType.STRING


async def test_writes_target_the_value_field_not_the_actuator_target():
    from kuksa_client.grpc import Field

    state = BridgeState()
    await state.put_latest(Direction.TO_VSS, LV_PATH, "OFF")
    kuksa = FakeVSSClient(KUKSA_PATHS)
    await _run_briefly(
        run_kuksa_current_writer(
            _config(), state, kuksa_factory=lambda: kuksa, sleep=RecordingSleep(1)
        )
    )
    assert tuple(kuksa.set_calls[0][0].fields) == (Field.VALUE,)


async def test_a_successful_write_clears_the_buffer():
    state = BridgeState()
    await state.put_latest(Direction.TO_VSS, LV_PATH, "OFF")
    await _run_briefly(
        run_kuksa_current_writer(
            _config(), state, kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS),
            sleep=RecordingSleep(1),
        )
    )
    _, pending = await state.pending_snapshot(Direction.TO_VSS)
    assert pending == {}


async def test_a_failed_write_leaves_the_buffer_intact():
    """Unacknowledged values are what makes the flush-on-reconnect work."""
    state = BridgeState()
    await state.put_latest(Direction.TO_VSS, LV_PATH, "OFF")
    await _run_briefly(
        run_kuksa_current_writer(
            _config(), state,
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS, set_error=RuntimeError("UNAVAILABLE")),
            sleep=RecordingSleep(2),
        )
    )
    _, pending = await state.pending_snapshot(Direction.TO_VSS)
    assert pending == {LV_PATH: "OFF"}


async def test_the_writer_flushes_the_backlog_on_reconnect():
    """The end-to-end reason the buffer exists."""
    state = BridgeState()
    attempts = {"n": 0}
    good = FakeVSSClient(KUKSA_PATHS)

    def factory():
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise ConnectionError("kuksa down")
        return good

    await state.put_latest(Direction.TO_VSS, LV_PATH, "OFF")
    await state.put_latest(Direction.TO_VSS, CHILD_PATH, "DETECTED")
    await _run_briefly(
        run_kuksa_current_writer(
            _config(), state, kuksa_factory=factory, sleep=RecordingSleep(3)
        )
    )
    assert len(good.set_calls) >= 1
    assert {u.entry.path for u in good.set_calls[0]} == {LV_PATH, CHILD_PATH}


async def test_the_writer_waits_rather_than_spinning_on_an_empty_buffer():
    state = BridgeState()
    kuksa = FakeVSSClient(KUKSA_PATHS)
    task = asyncio.create_task(
        run_kuksa_current_writer(
            _config(), state, kuksa_factory=lambda: kuksa, sleep=RecordingSleep()
        )
    )
    await asyncio.sleep(0.1)
    assert kuksa.set_calls == []

    await state.put_latest(Direction.TO_VSS, LV_PATH, "OFF")
    await asyncio.sleep(0.1)
    assert len(kuksa.set_calls) == 1

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_the_writer_records_its_connection_state():
    state = BridgeState()
    await state.put_latest(Direction.TO_VSS, LV_PATH, "OFF")
    await _run_briefly(
        run_kuksa_current_writer(
            _config(), state, kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS),
            sleep=RecordingSleep(1),
        )
    )
    snapshot = await state.snapshot()
    assert snapshot["mapping"]["to_vss_writes"] == 1


async def test_writer_cancellation_propagates():
    async def never(_):
        await asyncio.Event().wait()

    task = asyncio.create_task(
        run_kuksa_current_writer(
            _config(), BridgeState(), kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS),
            sleep=never,
        )
    )
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


# ── the seam ─────────────────────────────────────────────────────────────────


async def test_the_reader_keeps_running_while_kuksa_is_down():
    """The property the whole two-worker split exists for."""
    state = BridgeState()
    broker = _broker(
        batches=[[signal(NS, LV, 2)], [signal(NS, LV, 4)]], hang_after_batches=True
    )
    reader = asyncio.create_task(
        run_remotive_reader(
            _config(), state, broker_factory=lambda: broker,
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=RecordingSleep(),
        )
    )
    writer = asyncio.create_task(
        run_kuksa_current_writer(
            _config(), state,
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS, connect_error=ConnectionError("down")),
            sleep=RecordingSleep(),
        )
    )
    await asyncio.sleep(0.4)

    _, pending = await state.pending_snapshot(Direction.TO_VSS)
    assert pending == {LV_PATH: "ON"}  # newest CAN value, still buffered

    for task in (reader, writer):
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_newer_can_values_replace_older_while_kuksa_is_down():
    """Bounded: the buffer holds one slot per path, not a growing queue."""
    state = BridgeState()
    for value in ("UNDEFINED", "LOCK", "OFF", "ACC", "ON"):
        await state.put_latest(Direction.TO_VSS, LV_PATH, value)
    _, pending = await state.pending_snapshot(Direction.TO_VSS)
    assert pending == {LV_PATH: "ON"}
