"""Tests for Loop B — VSS to Remotive.

Mirror image of Loop A: a KUKSA target reader and a Remotive restbus writer,
joined by the same bounded buffer. The asymmetries are what these tests are
mostly about, and all three come from measurement:

* **`add()` happens once per connection, with every frame in one call.** It is
  destructive per client, so incremental adds would erase each other.
* **`get_target_values()` seeds before subscribing.** A target set while the
  bridge was down would otherwise never be delivered — `subscribe_target_values`
  only reports changes.
* **`allow_add: false` frames are still written.** `update_signals` works if an
  ECU already added the frame; validation warns when nothing has.
"""

from __future__ import annotations

import asyncio

import pytest
import structlog
from kuksa_client.grpc import Datapoint, Field, View, VSSClientError

from kx_vss_bridge.config import load_config_text
from kx_vss_bridge.state import BridgeState, Direction, Peer
from kx_vss_bridge.vss_to_remotive import (
    run_kuksa_target_reader,
    run_remotive_restbus_writer,
    run_vss_to_remotive,
)
from tests.fakes import (
    FakeBrokerClient,
    FakeVSSClient,
    RecordingSleep,
    make_frame_info,
)

NS = "BCM-VehicleCAN"
STATE = "VSS_VehicleState"
HMI = "VC_To_HMI"
HORN = f"{STATE}.Vehicle_Body_Horn_IsActive"
HAZARD = f"{STATE}.Vehicle_Body_Lights_Hazard_IsSignaling"
TELLTALE = f"{HMI}.TelltaleId"

HORN_PATH = "Vehicle.Body.Horn.IsActive"
HAZARD_PATH = "Vehicle.Body.Lights.Hazard.IsSignaling"
TELLTALE_PATH = "Vehicle.Cabin.HMI.TelltaleId"

CONFIG = f"""
remotive: {{url: http://broker:50051}}
kuksa: {{host: kuksa, port: 55557}}
options: {{seed_seconds: 0.05, retry_delay: 7}}
to_vss:
  - can: {{namespace: {NS}, signal: {STATE}.Vehicle_LowVoltageSystemState}}
    vss: Vehicle.LowVoltageSystemState
    type: string
    transform:
      op: enum
      map: {{0: UNDEFINED, 1: LOCK, 2: "OFF", 3: ACC, 4: "ON", 5: START}}
to_can:
  - vss: {HORN_PATH}
    can: {{namespace: {NS}, signal: {HORN}}}
    type: boolean
    transform: {{op: threshold, gt: 0}}
  - vss: {HAZARD_PATH}
    can: {{namespace: {NS}, signal: {HAZARD}}}
    type: boolean
    transform: {{op: threshold, gt: 0}}
  - vss: {TELLTALE_PATH}
    can: {{namespace: {NS}, signal: {TELLTALE}}}
    type: int
"""

KUKSA_PATHS = [
    "Vehicle.LowVoltageSystemState",
    HORN_PATH,
    HAZARD_PATH,
    TELLTALE_PATH,
]


def _config(text: str = CONFIG):
    return load_config_text(text).config


def _broker(**kwargs) -> FakeBrokerClient:
    kwargs.setdefault(
        "frame_infos",
        {
            NS: [
                make_frame_info(
                    STATE,
                    NS,
                    [f"{STATE}.Vehicle_LowVoltageSystemState", HORN, HAZARD],
                ),
                make_frame_info(HMI, NS, [TELLTALE]),
            ]
        },
    )
    return FakeBrokerClient(**kwargs)


async def _run_briefly(coro, timeout: float = 0.4) -> None:
    task = asyncio.create_task(coro)
    try:
        await asyncio.wait_for(task, timeout=timeout)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


# ── the target reader ────────────────────────────────────────────────────────


async def test_existing_targets_are_seeded_before_subscribing():
    """A target set while the bridge was down must still be delivered."""
    state = BridgeState()
    kuksa = FakeVSSClient(KUKSA_PATHS, target_values={HORN_PATH: Datapoint(True)})
    await _run_briefly(
        run_kuksa_target_reader(
            _config(), state, kuksa_factory=lambda: kuksa, sleep=RecordingSleep(1)
        )
    )
    _, pending = await state.pending_snapshot(Direction.TO_CAN)
    assert pending == {HORN_PATH: 1}


async def test_target_updates_are_inverted_before_buffering():
    state = BridgeState()
    kuksa = FakeVSSClient(
        KUKSA_PATHS,
        target_updates=[{HORN_PATH: Datapoint(True), TELLTALE_PATH: Datapoint(41)}],
    )
    await _run_briefly(
        run_kuksa_target_reader(
            _config(), state, kuksa_factory=lambda: kuksa, sleep=RecordingSleep(1)
        )
    )
    _, pending = await state.pending_snapshot(Direction.TO_CAN)
    assert pending == {HORN_PATH: 1, TELLTALE_PATH: 41}


async def test_a_null_datapoint_is_ignored():
    """An actuator with no target yet reads as None; that is not a value.

    Asserting the drop counter matters as much as the empty buffer: without the
    explicit guard the inversion still rejects None, but as a *failure*, so an
    untargeted actuator would inflate to_can_drops on every reconnect and make
    /stats look like a broken mapping.
    """
    state = BridgeState()
    kuksa = FakeVSSClient(
        KUKSA_PATHS,
        target_values={HORN_PATH: None},
        target_updates=[{HAZARD_PATH: Datapoint(None)}],
    )
    await _run_briefly(
        run_kuksa_target_reader(
            _config(), state, kuksa_factory=lambda: kuksa, sleep=RecordingSleep(1)
        )
    )
    _, pending = await state.pending_snapshot(Direction.TO_CAN)
    assert pending == {}
    assert (await state.snapshot())["mapping"]["to_can_drops"] == 0


async def test_a_v1_writer_reaches_can_even_though_v2_is_available():
    """The bridge must accept a target from a v1 writer. Measured, not assumed.

    cpd-core 1.0.0 pins kuksa-client==0.4.3, which contains no v2 code at all
    (checked: 0 occurrences of "v2" in its grpc module), so its
    `set_target_values` is a v1 Set. The CPD databroker — 0.7.1-dev, extracted
    from ghcr.io/manh-108/cpd-databroker:latest — serves BOTH
    /kuksa.val.v1.VAL/StreamedUpdate and /kuksa.val.v2.VAL/OpenProviderStream.

    That combination is the trap. `subscribe_target_values` in kuksa-client
    0.5.2 only falls back to v1 on UNIMPLEMENTED, and a broker serving v2 never
    answers UNIMPLEMENTED — so a v2-only subscriber waits forever on a stream
    nobody writes to, with no error on either side.

    Subscribing to both is what makes the bridge a bridge: it is not the
    bridge's business which protocol a writer chose.
    """
    state = BridgeState()
    kuksa = FakeVSSClient(
        KUKSA_PATHS,
        v1_target_updates=[{HORN_PATH: Datapoint(True)}],
    )
    await _run_briefly(
        run_kuksa_target_reader(
            _config(), state, kuksa_factory=lambda: kuksa, sleep=RecordingSleep(1)
        )
    )
    _, pending = await state.pending_snapshot(Direction.TO_CAN)
    assert pending == {HORN_PATH: 1}


async def test_v1_and_v2_writers_are_both_accepted_at_once():
    """Two writers on different protocols, one buffer. Neither starves the other.

    This is the shape the user asked for: more CPD instances change how many
    signals arrive, not how many protocols the bridge must understand.
    """
    state = BridgeState()
    kuksa = FakeVSSClient(
        KUKSA_PATHS,
        target_updates=[{TELLTALE_PATH: Datapoint(7)}],
        v1_target_updates=[{HORN_PATH: Datapoint(True)}],
    )
    await _run_briefly(
        run_kuksa_target_reader(
            _config(), state, kuksa_factory=lambda: kuksa, sleep=RecordingSleep(1)
        )
    )
    _, pending = await state.pending_snapshot(Direction.TO_CAN)
    assert pending == {HORN_PATH: 1, TELLTALE_PATH: 7}


async def test_the_v1_subscription_asks_for_target_values():
    """v1 needs View.TARGET_VALUE explicitly; the default view would give current."""
    state = BridgeState()
    kuksa = FakeVSSClient(KUKSA_PATHS)
    await _run_briefly(
        run_kuksa_target_reader(
            _config(), state, kuksa_factory=lambda: kuksa, sleep=RecordingSleep(1)
        )
    )
    assert kuksa.subscribe_entries, "the bridge never opened a v1 subscription"
    entries = kuksa.subscribe_entries[0]
    assert {e.path for e in entries} == set(_config().to_can_by_vss)
    assert all(e.view == View.TARGET_VALUE for e in entries)
    assert all(Field.ACTUATOR_TARGET in e.fields for e in entries)


async def test_one_protocol_failing_does_not_silence_the_other():
    """A broker that rejects v1 must still deliver v2 targets, and vice versa.

    Without this, adding the second subscription would make the bridge STRICTLY
    worse: one unsupported protocol would take down a direction that used to
    work.
    """
    state = BridgeState()
    kuksa = FakeVSSClient(
        KUKSA_PATHS,
        target_updates=[{TELLTALE_PATH: Datapoint(7)}],
        v1_subscribe_error=RuntimeError("v1 not served here"),
    )
    await _run_briefly(
        run_kuksa_target_reader(
            _config(), state, kuksa_factory=lambda: kuksa, sleep=RecordingSleep(1)
        )
    )
    _, pending = await state.pending_snapshot(Direction.TO_CAN)
    assert pending == {TELLTALE_PATH: 7}


async def test_an_unserved_protocol_is_not_reported_as_an_error():
    """UNIMPLEMENTED means "this broker has no such service" — ordinary, not a fault.

    The distinction earns its keep: subscribing to both protocols means one of
    them will legitimately be missing on many deployments. Counting that as an
    error would light up /stats permanently and train the operator to ignore it,
    which is how a REAL fault then goes unnoticed.
    """
    state = BridgeState()
    kuksa = FakeVSSClient(
        KUKSA_PATHS,
        target_updates=[{TELLTALE_PATH: Datapoint(7)}],
        v1_subscribe_error=VSSClientError(
            error={"code": 12, "reason": "unimplemented", "message": "no v1 here"},
            errors=[],
        ),
    )
    await _run_briefly(
        run_kuksa_target_reader(
            _config(), state, kuksa_factory=lambda: kuksa, sleep=RecordingSleep(1)
        )
    )
    snapshot = await state.snapshot()
    assert snapshot["kuksa"]["last_error"] is None
    assert snapshot["kuksa"]["errors"] == 0
    # ...and the protocol that IS served still delivered.
    _, pending = await state.pending_snapshot(Direction.TO_CAN)
    assert pending == {TELLTALE_PATH: 7}


async def test_a_real_failure_on_one_protocol_still_surfaces():
    """Anything that is not UNIMPLEMENTED must reach /stats.

    ALREADY_EXISTS (F4 — another provider owns the actuator) is the case that
    matters: the direction is genuinely not working and the operator has to be
    able to see why.
    """
    state = BridgeState()
    kuksa = FakeVSSClient(
        KUKSA_PATHS,
        v1_subscribe_error=RuntimeError("ALREADY_EXISTS: provider owns it"),
    )
    await _run_briefly(
        run_kuksa_target_reader(
            _config(), state, kuksa_factory=lambda: kuksa, sleep=RecordingSleep(1)
        )
    )
    snapshot = await state.snapshot()
    assert "ALREADY_EXISTS" in snapshot["kuksa"]["last_error"]


async def test_an_unmapped_target_path_is_ignored():
    state = BridgeState()
    kuksa = FakeVSSClient(
        KUKSA_PATHS, target_updates=[{"Vehicle.Not.Mapped": Datapoint(1)}]
    )
    await _run_briefly(
        run_kuksa_target_reader(
            _config(), state, kuksa_factory=lambda: kuksa, sleep=RecordingSleep(1)
        )
    )
    _, pending = await state.pending_snapshot(Direction.TO_CAN)
    assert pending == {}


async def test_an_uninvertible_target_drops_only_itself():
    """A value outside the declared range must not take its siblings down."""
    text = CONFIG.replace(
        f"""  - vss: {TELLTALE_PATH}
    can: {{namespace: {NS}, signal: {TELLTALE}}}
    type: int""",
        f"""  - vss: {TELLTALE_PATH}
    can: {{namespace: {NS}, signal: {TELLTALE}}}
    type: int
    range: {{min: 0, max: 10}}""",
    )
    state = BridgeState()
    kuksa = FakeVSSClient(
        KUKSA_PATHS,
        target_updates=[{TELLTALE_PATH: Datapoint(999), HORN_PATH: Datapoint(True)}],
    )
    await _run_briefly(
        run_kuksa_target_reader(
            _config(text), state, kuksa_factory=lambda: kuksa, sleep=RecordingSleep(1)
        )
    )
    _, pending = await state.pending_snapshot(Direction.TO_CAN)
    assert pending == {HORN_PATH: 1}
    assert (await state.snapshot())["mapping"]["to_can_drops"] >= 1


async def test_the_reader_subscribes_to_every_mapped_target():
    state = BridgeState()
    seen: list[list[str]] = []

    class Recording(FakeVSSClient):
        async def subscribe_target_values(self, paths, **kwargs):
            seen.append(sorted(paths))
            async for update in super().subscribe_target_values(paths, **kwargs):
                yield update

    await _run_briefly(
        run_kuksa_target_reader(
            _config(), state,
            kuksa_factory=lambda: Recording(KUKSA_PATHS),
            sleep=RecordingSleep(1),
        )
    )
    assert seen[0] == sorted([HORN_PATH, HAZARD_PATH, TELLTALE_PATH])


async def test_already_exists_marks_the_reader_degraded_without_crashing():
    """F4: another provider owns the actuator. Loop A must be unaffected."""
    state = BridgeState()
    await _run_briefly(
        run_kuksa_target_reader(
            _config(), state,
            kuksa_factory=lambda: FakeVSSClient(
                KUKSA_PATHS, subscribe_error=RuntimeError("ALREADY_EXISTS: provider")
            ),
            sleep=RecordingSleep(2),
        )
    )
    snapshot = await state.snapshot()
    assert "ALREADY_EXISTS" in snapshot["kuksa"]["last_error"]
    assert snapshot["remotive"]["connected"] is False  # untouched by this worker


async def test_the_reader_reseeds_after_a_reconnect():
    state = BridgeState()
    attempts = {"n": 0}

    def factory():
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise ConnectionError("kuksa down")
        return FakeVSSClient(KUKSA_PATHS, target_values={HORN_PATH: Datapoint(True)})

    await _run_briefly(
        run_kuksa_target_reader(
            _config(), state, kuksa_factory=factory, sleep=RecordingSleep(3)
        )
    )
    _, pending = await state.pending_snapshot(Direction.TO_CAN)
    assert pending == {HORN_PATH: 1}


async def test_reader_cancellation_propagates():
    async def never(_):
        await asyncio.Event().wait()

    task = asyncio.create_task(
        run_kuksa_target_reader(
            _config(), BridgeState(),
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=never,
        )
    )
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


# ── the restbus writer ───────────────────────────────────────────────────────


async def test_every_frame_is_added_in_exactly_one_call():
    """add() is destructive per client; two calls would erase the first.

    Uses two namespaces deliberately. With one, a loop over `add_args` and a
    single variadic call produce identical observations, so the bug this guards
    against would be invisible.
    """
    other_ns = "VC-VehicleCAN"
    text = CONFIG.replace(
        f"""  - vss: {TELLTALE_PATH}
    can: {{namespace: {NS}, signal: {TELLTALE}}}
    type: int""",
        f"""  - vss: {TELLTALE_PATH}
    can: {{namespace: {other_ns}, signal: {TELLTALE}}}
    type: int""",
    )
    broker = FakeBrokerClient(
        frame_infos={
            NS: [make_frame_info(STATE, NS, [HORN, HAZARD])],
            other_ns: [make_frame_info(HMI, other_ns, [TELLTALE])],
        }
    )
    state = BridgeState()
    await state.put_latest(Direction.TO_CAN, HORN_PATH, 1)
    await _run_briefly(
        run_remotive_restbus_writer(
            _config(text), state, broker_factory=lambda: broker,
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=RecordingSleep(1),
        )
    )
    # One call carrying both namespaces — not one call per namespace.
    assert len(broker.restbus.add_calls) == 1
    frames, start = broker.restbus.add_calls[0]
    assert start is True
    added = {(ns, cfg.name) for ns, configs in frames for cfg in configs}
    assert added == {(NS, STATE), (other_ns, HMI)}


async def test_restbus_writer_logs_connect_and_restbus_started():
    """Counts, not membership.

    Membership alone let a second `restbus started` inside the per-batch flush
    loop pass green — i.e. an event firing at CAN cycle rate would have shipped.
    All four are once-per-connection here because this test runs the *writer
    alone*; `kuksa connected` is 2 for a full `run_vss_to_remotive`, where the
    target reader owns its own KUKSA connection (see
    `test_full_loop_b_connects_to_kuksa_once_per_connection`). Counts observed,
    not assumed.
    """
    broker = _broker()
    with structlog.testing.capture_logs() as captured:
        await _run_briefly(
            run_remotive_restbus_writer(
                _config(), BridgeState(), broker_factory=lambda: broker,
                kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=RecordingSleep(1),
            )
        )
    events = [entry["event"] for entry in captured]
    assert events.count("remotive connected") == 1
    assert events.count("kuksa connected") == 1
    assert events.count("mapping validated") == 1
    assert events.count("restbus started") == 1


async def test_full_loop_b_connects_to_kuksa_once_per_connection():
    """Two KUKSA connections, so two `kuksa connected` — and that is correct.

    `run_kuksa_target_reader` and `run_remotive_restbus_writer` each own their
    own `VSSClient`. Pinning the count here is what stops someone "fixing" the
    duplicate by making the event fire once per *worker pair* — which would then
    hide a genuinely missing connection — and what stops the count drifting
    above 2 if either worker started reconnecting per batch.
    """
    broker = _broker()
    with structlog.testing.capture_logs() as captured:
        await _run_briefly(
            run_vss_to_remotive(
                _config(), BridgeState(), broker_factory=lambda: broker,
                kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=RecordingSleep(1),
            )
        )
    events = [entry["event"] for entry in captured]
    assert events.count("kuksa connected") == 2  # one per connection, not per loop
    assert events.count("remotive connected") == 1  # only the writer holds a broker
    assert events.count("restbus started") == 1


async def test_target_reader_logs_kuksa_connected_but_not_remotive():
    """This worker owns no broker connection, so it must not claim one."""
    kuksa = FakeVSSClient(KUKSA_PATHS)
    with structlog.testing.capture_logs() as captured:
        await _run_briefly(
            run_kuksa_target_reader(
                _config(), BridgeState(), kuksa_factory=lambda: kuksa,
                sleep=RecordingSleep(1),
            )
        )
    events = [entry["event"] for entry in captured]
    assert events.count("kuksa connected") == 1
    assert "remotive connected" not in events


async def test_added_frames_carry_the_real_cycle_time():
    broker = FakeBrokerClient(
        frame_infos={
            NS: [
                make_frame_info(STATE, NS, [HORN, HAZARD], cycle_time_millis=20.0),
                make_frame_info(HMI, NS, [TELLTALE], cycle_time_millis=50.0),
            ]
        }
    )
    state = BridgeState()
    await state.put_latest(Direction.TO_CAN, HORN_PATH, 1)
    await _run_briefly(
        run_remotive_restbus_writer(
            _config(), state, broker_factory=lambda: broker,
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=RecordingSleep(1),
        )
    )
    frames, _ = broker.restbus.add_calls[0]
    cycles = {cfg.name: cfg.cycle_time for _, configs in frames for cfg in configs}
    assert cycles == {STATE: 20.0, HMI: 50.0}


async def test_allow_add_false_frames_are_written_but_never_added():
    text = CONFIG.replace(
        f"""  - vss: {TELLTALE_PATH}
    can: {{namespace: {NS}, signal: {TELLTALE}}}
    type: int""",
        f"""  - vss: {TELLTALE_PATH}
    can: {{namespace: {NS}, signal: {TELLTALE}}}
    type: int
    allow_add: false""",
    )
    broker = _broker()
    state = BridgeState()
    await state.put_latest(Direction.TO_CAN, TELLTALE_PATH, 41)
    await _run_briefly(
        run_remotive_restbus_writer(
            _config(text), state, broker_factory=lambda: broker,
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=RecordingSleep(1),
        )
    )
    added = {
        cfg.name
        for frames, _ in broker.restbus.add_calls
        for _, configs in frames
        for cfg in configs
    }
    assert HMI not in added
    assert broker.restbus.update_calls  # still written


async def test_one_snapshot_becomes_one_update_call_grouped_by_namespace():
    broker = _broker()
    state = BridgeState()
    await state.put_latest(Direction.TO_CAN, HORN_PATH, 1)
    await state.put_latest(Direction.TO_CAN, HAZARD_PATH, 1)
    await _run_briefly(
        run_remotive_restbus_writer(
            _config(), state, broker_factory=lambda: broker,
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=RecordingSleep(1),
        )
    )
    assert len(broker.restbus.update_calls) == 1
    groups = broker.restbus.update_calls[0]
    assert len(groups) == 1  # one namespace
    namespace, configs = groups[0]
    assert namespace == NS
    assert {c.name for c in configs} == {HORN, HAZARD}


async def test_written_values_use_the_qualified_signal_name():
    broker = _broker()
    state = BridgeState()
    await state.put_latest(Direction.TO_CAN, HORN_PATH, 1)
    await _run_briefly(
        run_remotive_restbus_writer(
            _config(), state, broker_factory=lambda: broker,
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=RecordingSleep(1),
        )
    )
    config = broker.restbus.update_calls[0][0][1][0]
    assert config.name == HORN
    assert config.loop == [1]


async def test_a_successful_write_clears_the_buffer():
    state = BridgeState()
    await state.put_latest(Direction.TO_CAN, HORN_PATH, 1)
    await _run_briefly(
        run_remotive_restbus_writer(
            _config(), state, broker_factory=lambda: _broker(),
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=RecordingSleep(1),
        )
    )
    _, pending = await state.pending_snapshot(Direction.TO_CAN)
    assert pending == {}


async def test_a_failed_write_leaves_the_buffer_intact():
    broker = _broker()
    broker.restbus.fail_update_with = RuntimeError("broker gone")
    state = BridgeState()
    await state.put_latest(Direction.TO_CAN, HORN_PATH, 1)
    await _run_briefly(
        run_remotive_restbus_writer(
            _config(), state, broker_factory=lambda: broker,
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=RecordingSleep(2),
        )
    )
    _, pending = await state.pending_snapshot(Direction.TO_CAN)
    assert pending == {HORN_PATH: 1}


async def test_the_writer_re_adds_frames_after_a_reconnect():
    """Restbus configuration dies with the broker connection."""
    state = BridgeState()
    await state.put_latest(Direction.TO_CAN, HORN_PATH, 1)
    attempts = {"n": 0}
    second = _broker()

    def factory():
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise ConnectionError("broker down")
        return second

    await _run_briefly(
        run_remotive_restbus_writer(
            _config(), state, broker_factory=factory,
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=RecordingSleep(3),
        )
    )
    assert len(second.restbus.add_calls) == 1
    assert second.restbus.update_calls


async def test_the_writer_waits_rather_than_spinning_on_an_empty_buffer():
    broker = _broker()
    state = BridgeState()
    task = asyncio.create_task(
        run_remotive_restbus_writer(
            _config(), state, broker_factory=lambda: broker,
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=RecordingSleep(),
        )
    )
    await asyncio.sleep(0.15)
    assert broker.restbus.update_calls == []

    await state.put_latest(Direction.TO_CAN, HORN_PATH, 1)
    await asyncio.sleep(0.15)
    assert len(broker.restbus.update_calls) == 1

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_a_failing_add_is_retried_not_fatal():
    broker = _broker()
    broker.restbus.fail_add_with = RuntimeError("ALREADY_EXISTS")
    state = BridgeState()
    await state.put_latest(Direction.TO_CAN, HORN_PATH, 1)
    sleeper = RecordingSleep(stop_after=2)
    await _run_briefly(
        run_remotive_restbus_writer(
            _config(), state, broker_factory=lambda: broker,
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=sleeper,
        )
    )
    assert len(sleeper.delays) == 2
    assert all(d == 7 for d in sleeper.delays)


async def test_writes_are_counted():
    state = BridgeState()
    await state.put_latest(Direction.TO_CAN, HORN_PATH, 1)
    await state.put_latest(Direction.TO_CAN, HAZARD_PATH, 0)
    await _run_briefly(
        run_remotive_restbus_writer(
            _config(), state, broker_factory=lambda: _broker(),
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=RecordingSleep(1),
        )
    )
    assert (await state.snapshot())["mapping"]["to_can_writes"] == 2


async def test_writer_cancellation_propagates():
    async def never(_):
        await asyncio.Event().wait()

    task = asyncio.create_task(
        run_remotive_restbus_writer(
            _config(), BridgeState(), broker_factory=lambda: _broker(),
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=never,
        )
    )
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_shutdown_stops_the_frames_it_started():
    """The bridge must not keep transmitting after it exits.

    Measured on the live rig: `add(start=True)` makes the *broker* transmit the
    frame cyclically, and that outlives the client. A bridge that adds a frame to
    an otherwise-silent bus and then dies leaves 10 Hz of traffic behind carrying
    its last value — indistinguishable, to anyone downstream, from an ECU that is
    still running. A bridge is a conduit; when it stops, its effect stops.
    """
    broker = _broker()
    state = BridgeState()
    await state.put_latest(Direction.TO_CAN, HORN_PATH, 1)

    task = asyncio.create_task(
        run_remotive_restbus_writer(
            _config(), state, broker_factory=lambda: broker,
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=RecordingSleep(),
        )
    )
    await asyncio.sleep(0.2)
    assert broker.restbus.add_calls  # it did start transmitting

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert broker.restbus.closed == {NS}


async def test_shutdown_closes_every_namespace_it_added_to():
    other_ns = "VC-VehicleCAN"
    text = CONFIG.replace(
        f"""  - vss: {TELLTALE_PATH}
    can: {{namespace: {NS}, signal: {TELLTALE}}}
    type: int""",
        f"""  - vss: {TELLTALE_PATH}
    can: {{namespace: {other_ns}, signal: {TELLTALE}}}
    type: int""",
    )
    broker = FakeBrokerClient(
        frame_infos={
            NS: [make_frame_info(STATE, NS, [HORN, HAZARD])],
            other_ns: [make_frame_info(HMI, other_ns, [TELLTALE])],
        }
    )
    state = BridgeState()
    await state.put_latest(Direction.TO_CAN, HORN_PATH, 1)

    task = asyncio.create_task(
        run_remotive_restbus_writer(
            _config(text), state, broker_factory=lambda: broker,
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=RecordingSleep(),
        )
    )
    await asyncio.sleep(0.2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert broker.restbus.closed == {NS, other_ns}


async def test_nothing_is_closed_when_nothing_was_added():
    """allow_add: false means the frame was someone else's. Leave it alone."""
    text = CONFIG.replace("    type: boolean", "    type: boolean\n    allow_add: false")
    text = text.replace(
        f"    can: {{namespace: {NS}, signal: {TELLTALE}}}\n    type: int",
        f"    can: {{namespace: {NS}, signal: {TELLTALE}}}\n    type: int\n    allow_add: false",
    )
    config = _config(text)
    assert not any(m.allow_add for m in config.to_can)  # the premise

    broker = _broker()
    state = BridgeState()
    await state.put_latest(Direction.TO_CAN, HORN_PATH, 1)

    task = asyncio.create_task(
        run_remotive_restbus_writer(
            config, state, broker_factory=lambda: broker,
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=RecordingSleep(),
        )
    )
    await asyncio.sleep(0.2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert broker.restbus.add_calls == []
    assert broker.restbus.closed == set()


# ── the seam ─────────────────────────────────────────────────────────────────


async def test_the_target_reader_keeps_running_while_remotive_is_down():
    state = BridgeState()
    reader = asyncio.create_task(
        run_kuksa_target_reader(
            _config(), state,
            kuksa_factory=lambda: FakeVSSClient(
                KUKSA_PATHS, target_updates=[{HORN_PATH: Datapoint(True)}]
            ),
            sleep=RecordingSleep(),
        )
    )
    writer = asyncio.create_task(
        run_remotive_restbus_writer(
            _config(), state,
            broker_factory=lambda: FakeBrokerClient(connect_error=ConnectionError("down")),
            kuksa_factory=lambda: FakeVSSClient(KUKSA_PATHS), sleep=RecordingSleep(),
        )
    )
    await asyncio.sleep(0.3)

    _, pending = await state.pending_snapshot(Direction.TO_CAN)
    assert pending == {HORN_PATH: 1}

    for task in (reader, writer):
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
