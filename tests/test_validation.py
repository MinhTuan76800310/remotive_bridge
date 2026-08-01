"""Tests for live mapping validation.

Validation is what stops a typo becoming a silent no-op. It runs on every
Remotive connection and cross-checks the parsed mapping against what the vehicle
and the databroker actually contain: a signal that does not exist is dropped with
a reason, and a signal that exists but will behave surprisingly produces a
warning.

The warnings encode two findings measured against the live rig on 2026-08-01:

* **F9** — writing a frame an ECU also transmits makes the bridge a *second*
  transmitter, and the receiver sees the value alternate at cycle rate.
* **F10** — `update_signals` on a namespace whose restbus holds no such frame is
  silently ignored. No error, no delivery.

Neither blocks startup. Both are things an operator must be told.
"""

from __future__ import annotations

import pytest

from kx_vss_bridge.config import load_config_text
from kx_vss_bridge.validation import validate_mapping
from tests.fakes import FakeBrokerClient, FakeVSSClient, make_frame_info

BCM_NS = "BCM-VehicleCAN"
STATE = "VSS_VehicleState"
LV = f"{STATE}.Vehicle_LowVoltageSystemState"
HORN = f"{STATE}.Vehicle_Body_Horn_IsActive"

CONFIG = f"""
remotive: {{url: http://topology-broker.com:50051}}
kuksa: {{host: kuksa, port: 55557}}
to_vss:
  - can: {{namespace: {BCM_NS}, signal: {LV}}}
    vss: Vehicle.LowVoltageSystemState
    type: string
    transform:
      op: enum
      map: {{0: UNDEFINED, 1: LOCK, 2: "OFF", 3: ACC, 4: "ON", 5: START}}
to_can:
  - vss: Vehicle.Body.Horn.IsActive
    can: {{namespace: {BCM_NS}, signal: {HORN}}}
    type: boolean
    transform: {{op: threshold, gt: 0}}
"""

VSS_PATHS = ["Vehicle.LowVoltageSystemState", "Vehicle.Body.Horn.IsActive"]


def _config(text: str = CONFIG):
    return load_config_text(text).config


def _broker(sender: list[str] | None = None, cycle: float = 100.0) -> FakeBrokerClient:
    return FakeBrokerClient(
        frame_infos={
            BCM_NS: [
                make_frame_info(
                    STATE, BCM_NS, [LV, HORN], sender=sender, cycle_time_millis=cycle
                )
            ]
        }
    )


# ── the happy path ───────────────────────────────────────────────────────────


async def test_valid_mapping_is_kept_whole():
    result = await validate_mapping(_config(), _broker(), FakeVSSClient(VSS_PATHS))
    assert len(result.to_vss) == 1
    assert len(result.to_can) == 1
    assert not result.skipped


async def test_frame_infos_are_fetched_once_for_all_namespaces():
    """One round-trip per connection, not one per mapping entry."""
    broker = _broker()
    calls: list[tuple[str, ...]] = []
    original = broker.list_frame_infos

    async def spy(*namespaces: str):
        calls.append(namespaces)
        return await original(*namespaces)

    broker.list_frame_infos = spy  # type: ignore[method-assign]
    await validate_mapping(_config(), broker, FakeVSSClient(VSS_PATHS))
    assert len(calls) == 1
    assert set(calls[0]) == {BCM_NS}


async def test_metadata_is_fetched_in_one_batch():
    kuksa = FakeVSSClient(VSS_PATHS)
    await validate_mapping(_config(), _broker(), kuksa)
    assert len(kuksa.metadata_calls) == 1
    assert set(kuksa.metadata_calls[0]) == set(VSS_PATHS)


async def test_subscription_groups_are_grouped_by_namespace():
    result = await validate_mapping(_config(), _broker(), FakeVSSClient(VSS_PATHS))
    assert result.subscription_groups == ((BCM_NS, [LV]),)


async def test_restbus_frames_carry_the_real_cycle_time():
    """RestbusFrameConfig defaults cycleTime to 0.0, which is not transmitted."""
    result = await validate_mapping(_config(), _broker(cycle=20.0), FakeVSSClient(VSS_PATHS))
    assert result.restbus_frames[BCM_NS][STATE].cycle_time == 20.0


async def test_qualified_signal_keys_are_not_double_prefixed():
    """The live broker already returns 'Frame.Signal'; re-prefixing loses them."""
    result = await validate_mapping(_config(), _broker(), FakeVSSClient(VSS_PATHS))
    assert result.to_vss[0].can.signal == LV


# ── dropping what does not exist ─────────────────────────────────────────────


async def test_unknown_namespace_is_dropped_with_a_reason():
    text = CONFIG.replace(BCM_NS, "Ghost-CAN", 1)
    result = await validate_mapping(_config(text), _broker(), FakeVSSClient(VSS_PATHS))
    assert not result.to_vss
    assert "Ghost-CAN" in result.skipped[0]["reason"]


async def test_unknown_signal_is_dropped_but_siblings_survive():
    text = CONFIG.replace(LV, f"{STATE}.NoSuchSignal", 1)
    result = await validate_mapping(_config(text), _broker(), FakeVSSClient(VSS_PATHS))
    assert not result.to_vss
    assert len(result.to_can) == 1
    assert "NoSuchSignal" in result.skipped[0]["entry"]


async def test_unknown_vss_path_is_dropped():
    kuksa = FakeVSSClient(["Vehicle.LowVoltageSystemState"])  # horn absent
    result = await validate_mapping(_config(), _broker(), kuksa)
    assert len(result.to_vss) == 1
    assert not result.to_can
    assert "Vehicle.Body.Horn.IsActive" in result.skipped[0]["entry"]


async def test_a_failed_metadata_batch_falls_back_to_probing_each_path():
    """One unknown path must not take every mapping down with it."""
    kuksa = FakeVSSClient(
        ["Vehicle.LowVoltageSystemState"],
        metadata_batch_error=RuntimeError("NOT_FOUND: one or more paths"),
    )
    result = await validate_mapping(_config(), _broker(), kuksa)
    assert len(result.to_vss) == 1
    assert not result.to_can
    assert len(kuksa.metadata_calls) > 1


async def test_skipped_entries_are_reported_as_plain_dicts():
    """They go straight into /stats, so they must be JSON-friendly."""
    import json

    kuksa = FakeVSSClient(["Vehicle.LowVoltageSystemState"])
    result = await validate_mapping(_config(), _broker(), kuksa)
    json.dumps(result.skipped)
    assert set(result.skipped[0]) >= {"entry", "reason"}


# ── warnings: F9 and F10, both measured ──────────────────────────────────────


async def test_a_frame_with_a_sender_warns_about_duplicate_transmitters():
    """F9: measured — the value alternates between the two writers."""
    result = await validate_mapping(_config(), _broker(sender=["BCM"]), FakeVSSClient(VSS_PATHS))
    notes = " ".join(w["note"] for w in result.warnings)
    assert "BCM" in str(result.warnings)
    assert "transmit" in notes.lower()
    # A warning, never a block: the write demonstrably reaches the bus.
    assert len(result.to_can) == 1


async def test_allow_add_false_on_an_undriven_frame_warns_about_silent_no_op():
    """F10: measured — update_signals is silently ignored with no restbus."""
    text = CONFIG.replace(
        "transform: {op: threshold, gt: 0}",
        "transform: {op: threshold, gt: 0}\n    allow_add: false",
    )
    result = await validate_mapping(
        _config(text), _broker(sender=[]), FakeVSSClient(VSS_PATHS)
    )
    notes = " ".join(w["note"] for w in result.warnings)
    assert "silently" in notes.lower() or "ignored" in notes.lower()


async def test_no_no_op_warning_when_the_frame_is_driven_by_an_ecu():
    text = CONFIG.replace(
        "transform: {op: threshold, gt: 0}",
        "transform: {op: threshold, gt: 0}\n    allow_add: false",
    )
    result = await validate_mapping(
        _config(text), _broker(sender=["BCM"]), FakeVSSClient(VSS_PATHS)
    )
    notes = " ".join(w["note"] for w in result.warnings)
    assert "silently" not in notes.lower()


async def test_zero_cycle_time_warns_on_a_to_vss_entry():
    """Seeding cannot reach a frame that is never transmitted."""
    result = await validate_mapping(_config(), _broker(cycle=0.0), FakeVSSClient(VSS_PATHS))
    notes = " ".join(w["note"] for w in result.warnings)
    assert "cycle" in notes.lower()


async def test_a_clean_mapping_produces_no_warnings():
    result = await validate_mapping(_config(), _broker(), FakeVSSClient(VSS_PATHS))
    assert result.warnings == []


async def test_one_warning_per_frame_not_per_signal():
    """Found against the live rig: two signals on one frame warned twice.

    The condition is a property of the frame, so repeating it once per mapped
    signal is noise — and on a frame carrying eight signals it would bury
    everything else in /stats.
    """
    text = CONFIG.replace(
        """to_can:
  - vss: Vehicle.Body.Horn.IsActive
    can: {namespace: BCM-VehicleCAN, signal: VSS_VehicleState.Vehicle_Body_Horn_IsActive}
    type: boolean
    transform: {op: threshold, gt: 0}""",
        """to_can:
  - vss: Vehicle.Body.Horn.IsActive
    can: {namespace: BCM-VehicleCAN, signal: VSS_VehicleState.Vehicle_Body_Horn_IsActive}
    type: boolean
    transform: {op: threshold, gt: 0}
  - vss: Vehicle.Body.Lights.Hazard.IsSignaling
    can: {namespace: BCM-VehicleCAN, signal: VSS_VehicleState.Vehicle_Body_Lights_Hazard_IsSignaling}
    type: boolean
    transform: {op: threshold, gt: 0}""",
    )
    hazard = f"{STATE}.Vehicle_Body_Lights_Hazard_IsSignaling"
    broker = FakeBrokerClient(
        frame_infos={
            BCM_NS: [make_frame_info(STATE, BCM_NS, [LV, HORN, hazard], sender=["BCM"])]
        }
    )
    kuksa = FakeVSSClient(VSS_PATHS + ["Vehicle.Body.Lights.Hazard.IsSignaling"])
    result = await validate_mapping(_config(text), broker, kuksa)

    assert len(result.to_can) == 2
    assert len(result.warnings) == 1


# ── restbus frame selection ──────────────────────────────────────────────────


async def test_allow_add_false_frames_are_not_offered_to_add():
    text = CONFIG.replace(
        "transform: {op: threshold, gt: 0}",
        "transform: {op: threshold, gt: 0}\n    allow_add: false",
    )
    result = await validate_mapping(_config(text), _broker(), FakeVSSClient(VSS_PATHS))
    assert result.restbus_frames == {}
    # Still mapped: update_signals works if an ECU already added the frame.
    assert len(result.to_can) == 1


async def test_each_frame_appears_once_even_with_several_signals():
    text = CONFIG.replace(
        """to_can:
  - vss: Vehicle.Body.Horn.IsActive
    can: {namespace: BCM-VehicleCAN, signal: VSS_VehicleState.Vehicle_Body_Horn_IsActive}
    type: boolean
    transform: {op: threshold, gt: 0}""",
        """to_can:
  - vss: Vehicle.Body.Horn.IsActive
    can: {namespace: BCM-VehicleCAN, signal: VSS_VehicleState.Vehicle_Body_Horn_IsActive}
    type: boolean
    transform: {op: threshold, gt: 0}
  - vss: Vehicle.Body.Lights.Hazard.IsSignaling
    can: {namespace: BCM-VehicleCAN, signal: VSS_VehicleState.Vehicle_Body_Lights_Hazard_IsSignaling}
    type: boolean
    transform: {op: threshold, gt: 0}""",
    )
    hazard = f"{STATE}.Vehicle_Body_Lights_Hazard_IsSignaling"
    broker = FakeBrokerClient(
        frame_infos={BCM_NS: [make_frame_info(STATE, BCM_NS, [LV, HORN, hazard])]}
    )
    kuksa = FakeVSSClient(VSS_PATHS + ["Vehicle.Body.Lights.Hazard.IsSignaling"])
    result = await validate_mapping(_config(text), broker, kuksa)
    assert len(result.to_can) == 2
    assert list(result.restbus_frames[BCM_NS]) == [STATE]


# ── counts ───────────────────────────────────────────────────────────────────


async def test_active_counts_are_reported():
    result = await validate_mapping(_config(), _broker(), FakeVSSClient(VSS_PATHS))
    assert (result.active_to_vss, result.active_to_can) == (1, 1)


async def test_validation_result_is_immutable():
    result = await validate_mapping(_config(), _broker(), FakeVSSClient(VSS_PATHS))
    with pytest.raises((AttributeError, TypeError)):
        result.to_vss = ()  # type: ignore[misc]
