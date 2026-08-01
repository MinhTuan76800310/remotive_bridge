"""Tests for mapping configuration parsing.

The theme throughout: a *structural* problem (unreadable file, bad peer section,
nothing usable at all) refuses to start, because guessing would be worse. A
single *malformed entry* is reported and dropped, because 40 good mappings
should not be held hostage by one typo.
"""

from __future__ import annotations

import textwrap

import pytest

from kx_vss_bridge.config import (
    ConfigError,
    ValueType,
    load_config,
    load_config_text,
)

VALID = """
remotive:
  url: http://topology-broker.com:50051

kuksa:
  host: kuksa-databroker
  port: 55555

to_vss:
  - can: {namespace: BCM-VehicleCAN, signal: VSS_VehicleState.Vehicle_LowVoltageSystemState}
    vss: Vehicle.LowVoltageSystemState
    type: string
    transform:
      op: enum
      map: {0: UNDEFINED, 1: LOCK, 2: OFF, 3: ACC, 4: ON, 5: START}

to_can:
  - vss: Vehicle.Body.Horn.IsActive
    can: {namespace: BCM-VehicleCAN, signal: VSS_VehicleState.Vehicle_Body_Horn_IsActive}
    type: boolean
    transform: {op: threshold, gt: 0}
"""


def _with(section: str, body: str) -> str:
    """VALID's peer sections plus one mapping section, so each test varies one thing.

    Built by concatenation rather than an f-string inside dedent(): interpolation
    happens before dedent() sees the text, so the injected block's indentation no
    longer matches the template's and the result is not valid YAML.

    An always-valid entry is added to the *other* section. A config where nothing
    survives is a startup failure (covered separately by
    `test_config_with_no_usable_mapping_raises`), so without this anchor every
    single-bad-entry test would trip that rule instead of the one it targets.
    """
    entries = textwrap.indent(textwrap.dedent(body).strip("\n"), "  ")
    other = "to_can" if section == "to_vss" else "to_vss"
    anchor = (
        "  - vss: Vehicle.Anchor.Valid\n"
        "    can: {namespace: Anchor-CAN, signal: AnchorFrame.AnchorSignal}\n"
        "    type: float\n"
    )
    return (
        "remotive:\n"
        "  url: http://topology-broker.com:50051\n"
        "kuksa:\n"
        "  host: kuksa-databroker\n"
        "  port: 55555\n"
        f"{section}:\n"
        f"{entries}\n"
        f"{other}:\n"
        f"{anchor}"
    )


# ── the happy path ───────────────────────────────────────────────────────────


def test_valid_config_parses_both_directions():
    result = load_config_text(VALID)
    assert not result.skipped
    assert len(result.config.to_vss) == 1
    assert len(result.config.to_can) == 1


def test_can_signal_splits_frame_from_qualified_name():
    mapping = load_config_text(VALID).config.to_vss[0]
    assert mapping.can.frame == "VSS_VehicleState"
    assert mapping.can.signal == "VSS_VehicleState.Vehicle_LowVoltageSystemState"
    assert mapping.can.namespace == "BCM-VehicleCAN"


def test_defaults_are_applied():
    options = load_config_text(VALID).config.options
    assert options.seed_seconds == 3.0
    assert options.retry_delay == 10.0
    assert options.health_host == "0.0.0.0"
    assert options.health_port == 8090


def test_indexes_are_built_for_both_directions():
    config = load_config_text(VALID).config
    key = ("BCM-VehicleCAN", "VSS_VehicleState.Vehicle_LowVoltageSystemState")
    assert config.to_vss_by_can[key].vss == "Vehicle.LowVoltageSystemState"
    assert config.to_can_by_vss["Vehicle.Body.Horn.IsActive"].can.frame == "VSS_VehicleState"
    assert config.all_namespaces == ("BCM-VehicleCAN",)
    assert "Vehicle.LowVoltageSystemState" in config.all_vss_paths


def test_config_is_immutable():
    config = load_config_text(VALID).config
    with pytest.raises((AttributeError, TypeError)):
        config.to_vss[0].vss = "Vehicle.Other"  # type: ignore[misc]


# ── malformed entries: report and drop, keep the rest ────────────────────────


def test_type_is_mandatory_and_entry_is_skipped():
    raw = _with(
        "to_vss",
        """
        - can: {namespace: BCM-VehicleCAN, signal: Power.State}
          vss: Vehicle.LowVoltageSystemState
        - can: {namespace: BCM-VehicleCAN, signal: Speed.Value}
          vss: Vehicle.Speed
          type: float
        """,
    )
    result = load_config_text(raw)
    assert [m.vss for m in result.config.to_vss] == ["Vehicle.Speed"]
    assert len(result.skipped) == 1
    assert "type" in result.skipped[0].reason


def test_bare_signal_name_is_rejected():
    """RemotiveLabs needs Frame.Signal; a bare name is a silent NOT_FOUND."""
    raw = _with(
        "to_vss",
        """
        - can: {namespace: BCM-VehicleCAN, signal: BareName}
          vss: Vehicle.Speed
          type: float
        """,
    )
    result = load_config_text(raw)
    assert not result.config.to_vss
    assert "qualified" in result.skipped[0].reason.lower()


def test_unknown_transform_op_is_skipped():
    raw = _with(
        "to_vss",
        """
        - can: {namespace: BCM-VehicleCAN, signal: F.S}
          vss: Vehicle.Speed
          type: float
          transform: {op: quadratic}
        """,
    )
    result = load_config_text(raw)
    assert not result.config.to_vss
    assert "quadratic" in result.skipped[0].reason


def test_non_invertible_enum_is_skipped():
    """Loop B inverts by reverse lookup; duplicate outputs make that ambiguous."""
    raw = _with(
        "to_vss",
        """
        - can: {namespace: BCM-VehicleCAN, signal: F.S}
          vss: Vehicle.LowVoltageSystemState
          type: string
          transform:
            op: enum
            map: {0: OFF, 1: OFF}
        """,
    )
    result = load_config_text(raw)
    assert not result.config.to_vss
    assert "invert" in result.skipped[0].reason.lower()


def test_linear_with_zero_scale_is_skipped():
    raw = _with(
        "to_vss",
        """
        - can: {namespace: BCM-VehicleCAN, signal: F.S}
          vss: Vehicle.Speed
          type: float
          transform: {op: linear, scale: 0}
        """,
    )
    result = load_config_text(raw)
    assert not result.config.to_vss
    assert "scale" in result.skipped[0].reason


def test_inverted_range_is_skipped():
    raw = _with(
        "to_vss",
        """
        - can: {namespace: BCM-VehicleCAN, signal: F.S}
          vss: Vehicle.Speed
          type: float
          range: {min: 300, max: 0}
        """,
    )
    result = load_config_text(raw)
    assert not result.config.to_vss
    assert "range" in result.skipped[0].reason.lower()


def test_duplicate_can_source_is_skipped():
    raw = _with(
        "to_vss",
        """
        - can: {namespace: BCM-VehicleCAN, signal: F.S}
          vss: Vehicle.Speed
          type: float
        - can: {namespace: BCM-VehicleCAN, signal: F.S}
          vss: Vehicle.Other
          type: float
        """,
    )
    result = load_config_text(raw)
    assert len(result.config.to_vss) == 1
    assert "duplicate" in result.skipped[0].reason.lower()


def test_duplicate_vss_target_is_skipped():
    """Two writers for one actuator would fight; the second is dropped."""
    raw = _with(
        "to_can",
        """
        - vss: Vehicle.Body.Horn.IsActive
          can: {namespace: BCM-VehicleCAN, signal: F.A}
          type: boolean
        - vss: Vehicle.Body.Horn.IsActive
          can: {namespace: BCM-VehicleCAN, signal: F.B}
          type: boolean
        """,
    )
    result = load_config_text(raw)
    assert len(result.config.to_can) == 1
    assert "duplicate" in result.skipped[0].reason.lower()


def test_duplicate_can_destination_is_skipped():
    raw = _with(
        "to_can",
        """
        - vss: Vehicle.Body.Horn.IsActive
          can: {namespace: BCM-VehicleCAN, signal: F.A}
          type: boolean
        - vss: Vehicle.Body.Lights.Hazard.IsSignaling
          can: {namespace: BCM-VehicleCAN, signal: F.A}
          type: boolean
        """,
    )
    result = load_config_text(raw)
    assert len(result.config.to_can) == 1
    assert "duplicate" in result.skipped[0].reason.lower()


def test_skipped_entries_identify_their_section():
    raw = _with(
        "to_can",
        """
        - vss: Vehicle.Body.Horn.IsActive
          can: {namespace: BCM-VehicleCAN, signal: Bare}
          type: boolean
        """,
    )
    result = load_config_text(raw)
    assert result.skipped[0].section == "to_can"
    assert "Vehicle.Body.Horn.IsActive" in result.skipped[0].entry


# ── structural failures: refuse to start ─────────────────────────────────────


def test_unparseable_yaml_raises():
    with pytest.raises(ConfigError, match="parse"):
        load_config_text("remotive: [unclosed")


def test_non_mapping_document_raises():
    with pytest.raises(ConfigError):
        load_config_text("- just\n- a\n- list\n")


def test_missing_remotive_section_raises():
    with pytest.raises(ConfigError, match="remotive"):
        load_config_text("kuksa: {host: h, port: 55555}\nto_vss: []\n")


def test_missing_kuksa_section_raises():
    with pytest.raises(ConfigError, match="kuksa"):
        load_config_text("remotive: {url: http://b:50051}\nto_vss: []\n")


def test_unknown_top_level_key_raises():
    """A typo'd section would otherwise be silently ignored."""
    with pytest.raises(ConfigError, match="to_kuksa"):
        load_config_text(VALID + "\nto_kuksa: []\n")


def test_config_with_no_usable_mapping_raises():
    """A bridge that maps nothing is a misconfiguration, not a degraded state.

    Built inline rather than via `_with`, which always contributes a valid
    anchor entry precisely so the other tests do not hit this rule.
    """
    raw = (
        "remotive:\n"
        "  url: http://topology-broker.com:50051\n"
        "kuksa:\n"
        "  host: kuksa-databroker\n"
        "  port: 55555\n"
        "to_vss:\n"
        "  - can: {namespace: BCM-VehicleCAN, signal: Bare}\n"
        "    vss: Vehicle.Speed\n"
        "    type: float\n"
    )
    with pytest.raises(ConfigError, match="no valid"):
        load_config_text(raw)


@pytest.mark.parametrize("port", [0, -1, 70000, "abc"])
def test_invalid_kuksa_port_raises(port):
    raw = f"remotive: {{url: http://b:50051}}\nkuksa: {{host: h, port: {port}}}\nto_vss: []\n"
    with pytest.raises(ConfigError, match="port"):
        load_config_text(raw)


@pytest.mark.parametrize("field,value", [("seed_seconds", 0), ("retry_delay", -5)])
def test_non_positive_timings_raise(field, value):
    with pytest.raises(ConfigError, match=field):
        load_config_text(VALID + f"\noptions:\n  {field}: {value}\n")


def test_empty_remotive_url_raises():
    with pytest.raises(ConfigError, match="url"):
        load_config_text("remotive: {url: ''}\nkuksa: {host: h, port: 1}\nto_vss: []\n")


# ── YAML coercion traps ──────────────────────────────────────────────────────


def test_yaml_coerced_signal_name_is_rejected():
    """`signal: 123` parses as int; a silent str() would invent a signal name."""
    raw = _with(
        "to_vss",
        """
        - can: {namespace: BCM-VehicleCAN, signal: 123}
          vss: Vehicle.Speed
          type: float
        """,
    )
    result = load_config_text(raw)
    assert not result.config.to_vss


def test_yaml_norway_problem_in_enum_values_is_preserved():
    """Unquoted NO parses as False. An enum label must stay a string."""
    raw = _with(
        "to_vss",
        """
        - can: {namespace: BCM-VehicleCAN, signal: F.S}
          vss: Vehicle.LowVoltageSystemState
          type: string
          transform:
            op: enum
            map: {0: "NO", 1: "YES"}
        """,
    )
    result = load_config_text(raw)
    assert result.config.to_vss[0].transform.enum_map[0] == "NO"


def test_unknown_type_is_skipped():
    raw = _with(
        "to_vss",
        """
        - can: {namespace: BCM-VehicleCAN, signal: F.S}
          vss: Vehicle.Speed
          type: complex
        """,
    )
    result = load_config_text(raw)
    assert not result.config.to_vss
    assert "complex" in result.skipped[0].reason


def test_all_four_types_are_accepted():
    for name in ("boolean", "string", "int", "float"):
        raw = _with(
            "to_vss",
            f"""
            - can: {{namespace: BCM-VehicleCAN, signal: F.S}}
              vss: Vehicle.Speed
              type: {name}
            """,
        )
        result = load_config_text(raw)
        assert result.config.to_vss[0].value_type is ValueType(name)


# ── file loading ─────────────────────────────────────────────────────────────


def test_load_config_reads_a_file(tmp_path):
    path = tmp_path / "mapping.yaml"
    path.write_text(VALID)
    assert load_config(path).config.to_vss


def test_missing_file_raises(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "absent.yaml")


def test_token_is_kept_as_a_path_not_read(tmp_path):
    """The secret is loaded at client creation, so it never reaches /stats."""
    raw = VALID.replace(
        "  port: 55555", "  port: 55555\n  token: /run/secrets/kuksa.jwt"
    )
    kuksa = load_config_text(raw).config.kuksa
    assert str(kuksa.token_path) == "/run/secrets/kuksa.jwt"
