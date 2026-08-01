"""Tests for value coercion and transforms.

Two properties matter more than any single case:

* **Nothing is silently altered.** Truncation, overflow and YAML's truthiness
  quirks are all rejected, because a CAN signal that arrives as 12 when the
  vehicle sent 12.7 is worse than one that visibly fails.
* **Every forward transform has a faithful inverse.** Loop B runs the mapping
  backwards, so `inverse(forward(x)) == x` is a correctness requirement, not a
  nicety.
"""

from __future__ import annotations

import math

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from kx_vss_bridge.config import (
    CanSignal,
    Mapping,
    TransformOp,
    TransformSpec,
    ValueRange,
    ValueType,
)
from kx_vss_bridge.transform import TransformError, can_to_vss, coerce_value, vss_to_can


def _mapping(
    value_type: ValueType = ValueType.FLOAT,
    transform: TransformSpec | None = None,
    value_range: ValueRange | None = None,
) -> Mapping:
    return Mapping(
        can=CanSignal(namespace="NS", frame="F", signal="F.S"),
        vss="Vehicle.Test",
        value_type=value_type,
        transform=transform or TransformSpec(op=TransformOp.PASSTHROUGH),
        value_range=value_range,
        allow_add=True,
    )


# ── boolean coercion: the Datapoint("0") trap ────────────────────────────────


@pytest.mark.parametrize(
    "raw,expected",
    [
        (0, False),
        (1, True),
        (False, False),
        (True, True),
        ("0", False),
        ("1", True),
        ("false", False),
        ("true", True),
        ("False", False),
        ("True", True),
    ],
)
def test_boolean_coercion(raw, expected):
    assert coerce_value(raw, ValueType.BOOLEAN) is expected


def test_string_zero_is_not_truthy_boolean():
    """kuksa-client treats Datapoint("0") as True. A raw CAN 0 must not invert."""
    assert coerce_value("0", ValueType.BOOLEAN) is False


@pytest.mark.parametrize("raw", ["yes", "no", "on", "off", "", "2", 2, -1, 1.5])
def test_ambiguous_boolean_is_rejected(raw):
    with pytest.raises(TransformError):
        coerce_value(raw, ValueType.BOOLEAN)


# ── numeric coercion: no silent truncation or overflow ───────────────────────


def test_int_accepts_integral_float():
    assert coerce_value(12.0, ValueType.INT) == 12


def test_int_rejects_fractional_value():
    """kuksa-client would truncate 12.7 to 12 without a word."""
    with pytest.raises(TransformError, match="integral"):
        coerce_value(12.7, ValueType.INT)


def test_bool_is_not_a_number():
    """bool subclasses int in Python; accepting it here would mask type errors."""
    with pytest.raises(TransformError):
        coerce_value(True, ValueType.INT)


@pytest.mark.parametrize("raw", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_floats_are_rejected(raw):
    with pytest.raises(TransformError, match="finite"):
        coerce_value(raw, ValueType.FLOAT)


def test_float_accepts_numeric_string():
    assert coerce_value("12.5", ValueType.FLOAT) == 12.5


def test_float_rejects_non_numeric_string():
    with pytest.raises(TransformError):
        coerce_value("abc", ValueType.FLOAT)


def test_string_coercion_stringifies_numbers():
    assert coerce_value(42, ValueType.STRING) == "42"


def test_none_is_always_rejected():
    for value_type in ValueType:
        with pytest.raises(TransformError, match="null"):
            coerce_value(None, value_type)


# ── passthrough ──────────────────────────────────────────────────────────────


def test_passthrough_coerces_only():
    mapping = _mapping(ValueType.FLOAT)
    assert can_to_vss(mapping, 3) == 3.0
    assert vss_to_can(mapping, 3.0) == 3.0


# ── linear ───────────────────────────────────────────────────────────────────


def test_linear_forward_applies_scale_and_offset():
    mapping = _mapping(transform=TransformSpec(op=TransformOp.LINEAR, scale=0.01, offset=5))
    assert can_to_vss(mapping, 1000) == pytest.approx(15.0)


def test_linear_inverse_undoes_forward():
    mapping = _mapping(transform=TransformSpec(op=TransformOp.LINEAR, scale=0.01, offset=5))
    assert vss_to_can(mapping, 15.0) == pytest.approx(1000.0)


@settings(max_examples=200)
@given(
    raw=st.floats(min_value=-1e6, max_value=1e6, allow_nan=False, allow_infinity=False),
    scale=st.floats(min_value=0.001, max_value=1000, allow_nan=False, allow_infinity=False),
    offset=st.floats(min_value=-1e4, max_value=1e4, allow_nan=False, allow_infinity=False),
)
def test_linear_round_trip(raw, scale, offset):
    mapping = _mapping(transform=TransformSpec(op=TransformOp.LINEAR, scale=scale, offset=offset))
    assert vss_to_can(mapping, can_to_vss(mapping, raw)) == pytest.approx(raw, rel=1e-6, abs=1e-6)


# ── threshold ────────────────────────────────────────────────────────────────


def test_threshold_forward_compares():
    mapping = _mapping(ValueType.BOOLEAN, TransformSpec(op=TransformOp.THRESHOLD, gt=0))
    assert can_to_vss(mapping, 1) is True
    assert can_to_vss(mapping, 0) is False


def test_threshold_inverse_uses_configured_values():
    """Threshold is lossy, so the inverse maps to configured levels, not maths."""
    mapping = _mapping(
        ValueType.BOOLEAN,
        TransformSpec(op=TransformOp.THRESHOLD, gt=0, true_value=1, false_value=0),
    )
    assert vss_to_can(mapping, True) == 1
    assert vss_to_can(mapping, False) == 0


def test_threshold_inverse_honours_custom_levels():
    mapping = _mapping(
        ValueType.BOOLEAN,
        TransformSpec(op=TransformOp.THRESHOLD, gt=2, true_value=5, false_value=1),
    )
    assert vss_to_can(mapping, True) == 5
    assert vss_to_can(mapping, False) == 1


def test_threshold_forward_rejects_non_numeric_can_value():
    mapping = _mapping(ValueType.BOOLEAN, TransformSpec(op=TransformOp.THRESHOLD, gt=0))
    with pytest.raises(TransformError):
        can_to_vss(mapping, "abc")


# ── enum ─────────────────────────────────────────────────────────────────────


LV_STATES = {0: "UNDEFINED", 1: "LOCK", 2: "OFF", 3: "ACC", 4: "ON", 5: "START"}


def _enum_mapping() -> Mapping:
    return _mapping(
        ValueType.STRING,
        TransformSpec(
            op=TransformOp.ENUM,
            enum_map=dict(LV_STATES),
            enum_inverse={v: k for k, v in LV_STATES.items()},
        ),
    )


def test_enum_forward_maps_raw_to_label():
    assert can_to_vss(_enum_mapping(), 2) == "OFF"


def test_enum_inverse_maps_label_to_raw():
    assert vss_to_can(_enum_mapping(), "OFF") == 2


def test_enum_forward_tolerates_float_from_can():
    """CAN values arrive as float even for integral enum codes."""
    assert can_to_vss(_enum_mapping(), 2.0) == "OFF"


def test_unmapped_enum_raw_is_rejected():
    with pytest.raises(TransformError, match="not in transform.map"):
        can_to_vss(_enum_mapping(), 99)


def test_unmapped_enum_label_is_rejected():
    """Guessing a CAN code for an unknown label could actuate the wrong thing."""
    with pytest.raises(TransformError, match="not in transform.map"):
        vss_to_can(_enum_mapping(), "SPACESHIP")


@pytest.mark.parametrize("raw,label", sorted(LV_STATES.items()))
def test_enum_round_trip(raw, label):
    mapping = _enum_mapping()
    assert vss_to_can(mapping, can_to_vss(mapping, raw)) == raw
    assert can_to_vss(mapping, vss_to_can(mapping, label)) == label


# ── range ────────────────────────────────────────────────────────────────────


def test_value_inside_range_passes():
    mapping = _mapping(value_range=ValueRange(min=0, max=300))
    assert can_to_vss(mapping, 150) == 150.0


@pytest.mark.parametrize("raw", [-1, 301])
def test_value_outside_range_is_rejected(raw):
    """kuksa-client does not range-check; 300 into a UINT8 reaches the broker."""
    mapping = _mapping(value_range=ValueRange(min=0, max=300))
    with pytest.raises(TransformError, match="range"):
        can_to_vss(mapping, raw)


def test_range_is_checked_after_the_transform():
    """The range describes the value that is actually sent, not the raw input."""
    mapping = _mapping(
        transform=TransformSpec(op=TransformOp.LINEAR, scale=0.01),
        value_range=ValueRange(min=0, max=300),
    )
    assert can_to_vss(mapping, 10_000) == pytest.approx(100.0)
    with pytest.raises(TransformError, match="range"):
        can_to_vss(mapping, 40_000)


def test_range_applies_to_the_can_side_too():
    mapping = _mapping(
        transform=TransformSpec(op=TransformOp.LINEAR, scale=0.01),
        value_range=ValueRange(min=0, max=300),
    )
    with pytest.raises(TransformError, match="range"):
        vss_to_can(mapping, 400)


def test_range_is_not_applied_to_strings():
    """A range on a string mapping is meaningless, not a reason to fail."""
    mapping = _mapping(ValueType.STRING, value_range=ValueRange(min=0, max=10))
    assert can_to_vss(mapping, "hello") == "hello"


# ── determinism ──────────────────────────────────────────────────────────────


def test_transforms_are_pure():
    mapping = _mapping(transform=TransformSpec(op=TransformOp.LINEAR, scale=2.0))
    assert [can_to_vss(mapping, 21) for _ in range(3)] == [42.0, 42.0, 42.0]
