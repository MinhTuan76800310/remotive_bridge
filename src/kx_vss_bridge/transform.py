"""Value coercion and transforms — pure functions, no I/O.

`can_to_vss` runs a mapping forwards; `vss_to_can` runs it backwards. Loop B
depends on the inverse being faithful, so every transform that can be inverted
is, and the one that cannot — `threshold`, which discards magnitude — inverts to
explicitly configured levels rather than pretending to reconstruct the input.

Everything here raises `TransformError` and never logs or counts. Callers own
the drop counters, because only they know which direction and which peer.

The strictness is deliberate. Measured on kuksa-client 0.5.2:

* `Datapoint("0")` is truthy — only "False"/"false"/"F"/"f" are falsy — so an
  untyped CAN 0 would invert the meaning of every boolean.
* `12.7` into a UINT8 silently becomes `12`, and `300` passes the client
  untouched and is rejected by the broker with INVALID_ARGUMENT.

A value that cannot be represented faithfully is dropped loudly instead.
"""

from __future__ import annotations

import math
from typing import Any

from kx_vss_bridge.config import Mapping, TransformOp, ValueType

__all__ = ["TransformError", "can_to_vss", "coerce_value", "vss_to_can"]

# Accepted spellings for booleans arriving as text. Deliberately narrow: "yes",
# "on" and "2" are rejected rather than guessed, since a wrong guess here silently
# actuates the wrong thing.
_TRUE_STRINGS = frozenset({"1", "true", "t"})
_FALSE_STRINGS = frozenset({"0", "false", "f"})


class TransformError(Exception):
    """This value cannot be represented faithfully and must be dropped."""


def _as_number(value: Any, what: str) -> float:
    """A finite float, or an error. bool is excluded: it subclasses int."""
    if isinstance(value, bool):
        raise TransformError(f"{what} expected a number, got boolean {value!r}")
    if isinstance(value, str):
        try:
            value = float(value)
        except ValueError:
            raise TransformError(f"{what} expected a number, got {value!r}") from None
    if not isinstance(value, (int, float)):
        raise TransformError(f"{what} expected a number, got {type(value).__name__}")
    number = float(value)
    if not math.isfinite(number):
        # NaN and ±inf have no CAN or VSS representation, and NaN would poison
        # every downstream comparison.
        raise TransformError(f"{what} expected a finite number, got {number!r}")
    return number


def coerce_value(value: Any, value_type: ValueType) -> bool | str | int | float:
    """Convert to the declared VSS type, refusing anything lossy or ambiguous."""
    if value is None:
        raise TransformError("value is null")

    if value_type is ValueType.BOOLEAN:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in _TRUE_STRINGS:
                return True
            if lowered in _FALSE_STRINGS:
                return False
            raise TransformError(f"cannot read {value!r} as a boolean")
        if isinstance(value, (int, float)):
            # Only exactly 0 or 1. A CAN signal carrying 2 into a boolean means
            # the mapping is wrong, and coercing it would hide that.
            if value == 0:
                return False
            if value == 1:
                return True
            raise TransformError(f"cannot read {value!r} as a boolean; expected 0 or 1")
        raise TransformError(f"cannot read {type(value).__name__} as a boolean")

    if value_type is ValueType.STRING:
        if isinstance(value, str):
            return value
        if isinstance(value, bool):
            raise TransformError(f"refusing to stringify boolean {value!r}")
        if isinstance(value, (int, float)):
            return str(value)
        raise TransformError(f"cannot read {type(value).__name__} as a string")

    if value_type is ValueType.INT:
        number = _as_number(value, "int")
        if not float(number).is_integer():
            raise TransformError(f"{number!r} is not integral; refusing to truncate")
        return int(number)

    return _as_number(value, "float")


def _check_range(value: Any, mapping: Mapping) -> None:
    """Enforce the declared range on the value about to leave the bridge.

    Skipped for non-numeric values: a range on a string mapping is meaningless
    configuration, not a reason to drop live data.
    """
    if mapping.value_range is None or isinstance(value, (str, bool)):
        return
    low, high = mapping.value_range.min, mapping.value_range.max
    if not low <= value <= high:
        raise TransformError(f"{value!r} outside configured range [{low}, {high}]")


def can_to_vss(mapping: Mapping, value: Any) -> bool | str | int | float:
    """Run a mapping forwards: a raw CAN value becomes a typed VSS value."""
    spec = mapping.transform

    if spec.op is TransformOp.LINEAR:
        value = _as_number(value, "linear") * spec.scale + spec.offset

    elif spec.op is TransformOp.THRESHOLD:
        value = _as_number(value, "threshold") > spec.gt

    elif spec.op is TransformOp.ENUM:
        key = value
        if isinstance(key, float) and key.is_integer():
            # CAN values arrive as float even for integral enum codes, and the
            # map is keyed by the ints YAML parsed.
            key = int(key)
        if key not in spec.enum_map:
            raise TransformError(f"CAN value {value!r} is not in transform.map")
        value = spec.enum_map[key]

    result = coerce_value(value, mapping.value_type)
    _check_range(result, mapping)
    return result


def vss_to_can(mapping: Mapping, value: Any) -> bool | str | int | float:
    """Run a mapping backwards: a VSS value becomes the raw CAN value to write."""
    spec = mapping.transform

    if spec.op is TransformOp.LINEAR:
        result: Any = (_as_number(value, "linear") - spec.offset) / spec.scale

    elif spec.op is TransformOp.THRESHOLD:
        # Not a mathematical inverse — the forward direction threw the magnitude
        # away. The configured levels are the honest reconstruction.
        result = spec.true_value if coerce_value(value, ValueType.BOOLEAN) else spec.false_value

    elif spec.op is TransformOp.ENUM:
        if value not in spec.enum_inverse:
            raise TransformError(f"VSS value {value!r} is not in transform.map")
        result = spec.enum_inverse[value]

    else:
        result = coerce_value(value, mapping.value_type)

    _check_range(result, mapping)
    return result
