"""Parse and validate the mapping file into immutable, indexed configuration.

Two classes of problem, deliberately handled differently:

* A **structural** problem — the file is unreadable, a peer section is missing or
  nonsense, or nothing usable survives — raises `ConfigError` and the process
  refuses to start. There is no safe way to guess what was meant.
* A **single malformed entry** is recorded in `ConfigLoadResult.skipped` and
  dropped. Forty good mappings should not be held hostage by one typo, and the
  diagnostic is surfaced on `/stats` so it cannot be missed either.

Nothing here talks to a peer. Checking that a signal or VSS path actually exists
is `validation.py`'s job, against a live connection.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping as MappingABC

import yaml

__all__ = [
    "BridgeConfig",
    "CanSignal",
    "ConfigError",
    "ConfigLoadResult",
    "KuksaConfig",
    "Mapping",
    "Options",
    "RemotiveConfig",
    "SkippedMapping",
    "TransformOp",
    "TransformSpec",
    "ValueRange",
    "ValueType",
    "load_config",
    "load_config_text",
]

_MAX_PORT = 65535


class ConfigError(Exception):
    """The configuration cannot be used at all; the process must not start."""


class ValueType(enum.Enum):
    """The declared type of the value as VSS sees it.

    Mandatory on every mapping. Two measured reasons, both on kuksa-client 0.5.2:
    `Datapoint("0")` evaluates truthy (only "False"/"false"/"F"/"f" are falsy),
    so an untyped CAN 0 inverts every boolean; and without a declared type
    `set()` performs a metadata round-trip before *every* write.
    """

    BOOLEAN = "boolean"
    STRING = "string"
    INT = "int"
    FLOAT = "float"


class TransformOp(enum.Enum):
    PASSTHROUGH = "passthrough"
    LINEAR = "linear"
    THRESHOLD = "threshold"
    ENUM = "enum"


@dataclass(frozen=True)
class CanSignal:
    """A RemotiveLabs signal, always message-qualified.

    `signal` keeps the full `Frame.Signal` form because that is what the broker
    API expects; `frame` is split out because the restbus is configured per frame.
    """

    namespace: str
    frame: str
    signal: str


@dataclass(frozen=True)
class ValueRange:
    min: float
    max: float


@dataclass(frozen=True)
class TransformSpec:
    op: TransformOp
    scale: float = 1.0
    offset: float = 0.0
    gt: float = 0.0
    true_value: Any = 1
    false_value: Any = 0
    enum_map: MappingABC[Any, Any] = field(default_factory=dict)
    enum_inverse: MappingABC[Any, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Mapping:
    can: CanSignal
    vss: str
    value_type: ValueType
    transform: TransformSpec
    value_range: ValueRange | None
    allow_add: bool


@dataclass(frozen=True)
class RemotiveConfig:
    url: str


@dataclass(frozen=True)
class KuksaConfig:
    host: str
    port: int
    # A path, never the secret itself. Read at client creation so the token
    # cannot leak through a state snapshot or a log line.
    token_path: Path | None = None


@dataclass(frozen=True)
class Options:
    seed_seconds: float = 3.0
    retry_delay: float = 10.0
    health_host: str = "0.0.0.0"
    health_port: int = 8090


@dataclass(frozen=True)
class SkippedMapping:
    section: str
    entry: str
    reason: str


@dataclass(frozen=True)
class BridgeConfig:
    remotive: RemotiveConfig
    kuksa: KuksaConfig
    options: Options
    to_vss: tuple[Mapping, ...]
    to_can: tuple[Mapping, ...]

    @property
    def to_vss_by_can(self) -> dict[tuple[str, str], Mapping]:
        return {(m.can.namespace, m.can.signal): m for m in self.to_vss}

    @property
    def to_can_by_vss(self) -> dict[str, Mapping]:
        return {m.vss: m for m in self.to_can}

    @property
    def all_namespaces(self) -> tuple[str, ...]:
        seen = {m.can.namespace for m in self.to_vss} | {
            m.can.namespace for m in self.to_can
        }
        return tuple(sorted(seen))

    @property
    def all_vss_paths(self) -> tuple[str, ...]:
        seen = {m.vss for m in self.to_vss} | {m.vss for m in self.to_can}
        return tuple(sorted(seen))


@dataclass(frozen=True)
class ConfigLoadResult:
    """Usable configuration, plus what was thrown away and why."""

    config: BridgeConfig
    skipped: tuple[SkippedMapping, ...]


class _EntryError(Exception):
    """One mapping entry is unusable. Internal; becomes a SkippedMapping."""


# ── scalar helpers ───────────────────────────────────────────────────────────


def _require_str(value: Any, what: str) -> str:
    """Reject YAML's helpful coercions.

    `signal: 123` parses as int and `signal: NO` as False. Calling str() on
    either would invent a name that silently never matches.
    """
    if not isinstance(value, str):
        raise _EntryError(f"{what} must be a string, got {type(value).__name__}")
    if not value.strip():
        raise _EntryError(f"{what} must not be empty")
    return value


def _require_number(value: Any, what: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _EntryError(f"{what} must be a number, got {type(value).__name__}")
    return float(value)


def _positive(raw: MappingABC[str, Any], key: str, default: float) -> float:
    value = raw.get(key, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(f"options.{key} must be a number")
    if value <= 0:
        raise ConfigError(f"options.{key} must be greater than zero, got {value}")
    return float(value)


def _port(value: Any, what: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{what} must be an integer port, got {value!r}")
    if not 1 <= value <= _MAX_PORT:
        raise ConfigError(f"{what} must be between 1 and {_MAX_PORT}, got {value}")
    return value


# ── section parsers ──────────────────────────────────────────────────────────


def _parse_remotive(raw: Any) -> RemotiveConfig:
    if not isinstance(raw, MappingABC):
        raise ConfigError("remotive section is missing or not a mapping")
    url = raw.get("url")
    if not isinstance(url, str) or not url.strip():
        raise ConfigError("remotive.url must be a non-empty string")
    return RemotiveConfig(url=url)


def _parse_kuksa(raw: Any) -> KuksaConfig:
    if not isinstance(raw, MappingABC):
        raise ConfigError("kuksa section is missing or not a mapping")
    host = raw.get("host")
    if not isinstance(host, str) or not host.strip():
        raise ConfigError("kuksa.host must be a non-empty string")
    token = raw.get("token")
    if token is not None and not isinstance(token, str):
        raise ConfigError("kuksa.token must be a path to a token file")
    return KuksaConfig(
        host=host,
        port=_port(raw.get("port", 55555), "kuksa.port"),
        token_path=Path(token) if token else None,
    )


def _parse_options(raw: Any) -> Options:
    if raw is None:
        return Options()
    if not isinstance(raw, MappingABC):
        raise ConfigError("options section must be a mapping")
    unknown = set(raw) - {"seed_seconds", "retry_delay", "health_host", "health_port"}
    if unknown:
        raise ConfigError(f"unknown options key(s): {', '.join(sorted(unknown))}")
    host = raw.get("health_host", "0.0.0.0")
    if not isinstance(host, str) or not host.strip():
        raise ConfigError("options.health_host must be a non-empty string")
    return Options(
        seed_seconds=_positive(raw, "seed_seconds", 3.0),
        retry_delay=_positive(raw, "retry_delay", 10.0),
        health_host=host,
        health_port=_port(raw.get("health_port", 8090), "options.health_port"),
    )


def _parse_can_signal(raw: Any) -> CanSignal:
    if not isinstance(raw, MappingABC):
        raise _EntryError("can must be a mapping with namespace and signal")
    unknown = set(raw) - {"namespace", "signal"}
    if unknown:
        raise _EntryError(f"unknown can key(s): {', '.join(sorted(unknown))}")
    namespace = _require_str(raw.get("namespace"), "can.namespace")
    signal = _require_str(raw.get("signal"), "can.signal")
    # RemotiveLabs requires Frame.Signal. A bare name produces NOT_FOUND at
    # runtime, which is far harder to diagnose than refusing it here.
    frame, dot, remainder = signal.partition(".")
    if not dot or not frame or not remainder:
        raise _EntryError(
            f"can.signal must be message-qualified as 'Frame.Signal', got {signal!r}"
        )
    return CanSignal(namespace=namespace, frame=frame, signal=signal)


def _check_enum_value_type(key: Any, value: Any, value_type: ValueType) -> None:
    """Reject an enum output that cannot be the declared type.

    Mostly this catches YAML 1.1's truthiness rules. `ON` and `OFF` are two of
    the six `Vehicle.LowVoltageSystemState` values, and written naturally —
    unquoted — YAML parses them as booleans. The mapping then feeds `False` to a
    path the catalog declares as a string, and the failure surfaces far from its
    cause: either the databroker rejects the write, or the operator sees no alert
    and no error at all.

    The fix is one character, so the message says so.
    """
    if isinstance(value, bool) and value_type is not ValueType.BOOLEAN:
        raise _EntryError(
            f"transform.map[{key!r}] is the boolean {value!r}, but type is "
            f"{value_type.value!r}. YAML reads bare y/n/yes/no/on/off/true/false as "
            f"booleans — quote the value (e.g. \"{'ON' if value else 'OFF'}\") to keep "
            f"it a string"
        )

    if value_type is ValueType.STRING and not isinstance(value, str):
        raise _EntryError(
            f"transform.map[{key!r}] is {type(value).__name__} {value!r}, but type is "
            f"'string'; quote it to keep it a string"
        )

    if value_type in (ValueType.INT, ValueType.FLOAT) and not isinstance(
        value, (int, float)
    ):
        raise _EntryError(
            f"transform.map[{key!r}] is {value!r}, which is not a "
            f"{value_type.value}"
        )


def _parse_transform(raw: Any, value_type: ValueType) -> TransformSpec:
    if raw is None:
        return TransformSpec(op=TransformOp.PASSTHROUGH)
    if not isinstance(raw, MappingABC):
        raise _EntryError("transform must be a mapping")

    name = raw.get("op", "passthrough")
    if not isinstance(name, str):
        raise _EntryError(f"transform.op must be a string, got {name!r}")
    try:
        op = TransformOp(name)
    except ValueError:
        valid = ", ".join(o.value for o in TransformOp)
        raise _EntryError(f"unknown transform op {name!r}; expected one of {valid}")

    if op is TransformOp.LINEAR:
        scale = _require_number(raw.get("scale", 1.0), "transform.scale")
        if scale == 0:
            # The inverse divides by scale; zero makes the mapping one-way and
            # destroys the value.
            raise _EntryError("transform.scale must not be zero")
        return TransformSpec(
            op=op,
            scale=scale,
            offset=_require_number(raw.get("offset", 0.0), "transform.offset"),
        )

    if op is TransformOp.THRESHOLD:
        return TransformSpec(
            op=op,
            gt=_require_number(raw.get("gt", 0.0), "transform.gt"),
            true_value=raw.get("true_value", 1),
            false_value=raw.get("false_value", 0),
        )

    if op is TransformOp.ENUM:
        mapping = raw.get("map")
        if not isinstance(mapping, MappingABC) or not mapping:
            raise _EntryError("transform.map must be a non-empty mapping")
        forward = dict(mapping)
        inverse: dict[Any, Any] = {}
        for key, value in forward.items():
            _check_enum_value_type(key, value, value_type)
            if value in inverse:
                # Loop B inverts by reverse lookup. Two keys sharing a value
                # make that ambiguous, so refuse rather than pick one.
                raise _EntryError(
                    f"transform.map cannot be inverted: value {value!r} appears twice"
                )
            inverse[value] = key
        return TransformSpec(op=op, enum_map=forward, enum_inverse=inverse)

    return TransformSpec(op=TransformOp.PASSTHROUGH)


def _parse_range(raw: Any) -> ValueRange | None:
    if raw is None:
        return None
    if not isinstance(raw, MappingABC):
        raise _EntryError("range must be a mapping with min and max")
    unknown = set(raw) - {"min", "max"}
    if unknown:
        raise _EntryError(f"unknown range key(s): {', '.join(sorted(unknown))}")
    low = _require_number(raw.get("min"), "range.min")
    high = _require_number(raw.get("max"), "range.max")
    if low > high:
        raise _EntryError(f"range.min ({low}) must not exceed range.max ({high})")
    return ValueRange(min=low, max=high)


_ENTRY_KEYS = {"can", "vss", "type", "transform", "range", "allow_add"}


def _parse_entry(raw: Any, *, default_allow_add: bool) -> Mapping:
    if not isinstance(raw, MappingABC):
        raise _EntryError("entry must be a mapping")
    unknown = set(raw) - _ENTRY_KEYS
    if unknown:
        raise _EntryError(f"unknown key(s): {', '.join(sorted(unknown))}")

    type_name = raw.get("type")
    if type_name is None:
        raise _EntryError("missing required field: type")
    if not isinstance(type_name, str):
        raise _EntryError(f"type must be a string, got {type_name!r}")
    try:
        value_type = ValueType(type_name)
    except ValueError:
        valid = ", ".join(t.value for t in ValueType)
        raise _EntryError(f"unknown type {type_name!r}; expected one of {valid}")

    allow_add = raw.get("allow_add", default_allow_add)
    if not isinstance(allow_add, bool):
        raise _EntryError(f"allow_add must be true or false, got {allow_add!r}")

    return Mapping(
        can=_parse_can_signal(raw.get("can")),
        vss=_require_str(raw.get("vss"), "vss"),
        value_type=value_type,
        transform=_parse_transform(raw.get("transform"), value_type),
        value_range=_parse_range(raw.get("range")),
        allow_add=allow_add,
    )


def _describe(raw: Any) -> str:
    """A label for a rejected entry, for logs and /stats."""
    if isinstance(raw, MappingABC):
        vss = raw.get("vss")
        can = raw.get("can")
        signal = can.get("signal") if isinstance(can, MappingABC) else None
        parts = [str(p) for p in (vss, signal) if p is not None]
        if parts:
            return " -> ".join(parts)
    return "<unparseable entry>"


def _parse_section(
    raw: Any, section: str, *, default_allow_add: bool
) -> tuple[list[Mapping], list[SkippedMapping]]:
    if raw is None:
        return [], []
    if not isinstance(raw, list):
        raise ConfigError(f"{section} must be a list of mappings")

    kept: list[Mapping] = []
    skipped: list[SkippedMapping] = []
    seen_can: set[tuple[str, str]] = set()
    seen_vss: set[str] = set()

    for entry in raw:
        try:
            mapping = _parse_entry(entry, default_allow_add=default_allow_add)
        except _EntryError as exc:
            skipped.append(SkippedMapping(section, _describe(entry), str(exc)))
            continue

        can_key = (mapping.can.namespace, mapping.can.signal)
        if can_key in seen_can:
            skipped.append(
                SkippedMapping(
                    section,
                    _describe(entry),
                    f"duplicate CAN signal {mapping.can.signal!r} in {mapping.can.namespace!r}",
                )
            )
            continue
        if mapping.vss in seen_vss:
            skipped.append(
                SkippedMapping(
                    section, _describe(entry), f"duplicate VSS path {mapping.vss!r}"
                )
            )
            continue

        seen_can.add(can_key)
        seen_vss.add(mapping.vss)
        kept.append(mapping)

    return kept, skipped


# ── entry points ─────────────────────────────────────────────────────────────

_TOP_LEVEL_KEYS = {"remotive", "kuksa", "options", "to_vss", "to_can"}

# Whether the bridge may call Restbus.add() for a frame that has no explicit
# setting. `add()` is documented as removing any previous configuration on the
# namespace, and whether that is scoped per-client or per-namespace is unproven
# against a live broker (risk F1). Task 1's spike settles it; until then this
# default is the one line that changes.
DEFAULT_ALLOW_ADD = True


def load_config_text(text: str, *, default_allow_add: bool = DEFAULT_ALLOW_ADD) -> ConfigLoadResult:
    try:
        document = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ConfigError(f"could not parse mapping YAML: {exc}") from exc

    if not isinstance(document, MappingABC):
        raise ConfigError("mapping file must contain a top-level mapping")

    unknown = set(document) - _TOP_LEVEL_KEYS
    if unknown:
        raise ConfigError(f"unknown top-level key(s): {', '.join(sorted(unknown))}")

    # Peer sections first: a missing broker URL is a more fundamental error than
    # an empty mapping list, and reporting it second would hide it behind
    # "no valid mappings".
    remotive = _parse_remotive(document.get("remotive"))
    kuksa = _parse_kuksa(document.get("kuksa"))
    options = _parse_options(document.get("options"))

    to_vss, skipped_vss = _parse_section(
        document.get("to_vss"), "to_vss", default_allow_add=default_allow_add
    )
    to_can, skipped_can = _parse_section(
        document.get("to_can"), "to_can", default_allow_add=default_allow_add
    )

    if not to_vss and not to_can:
        raise ConfigError(
            "no valid mappings: the bridge would connect to both peers and do nothing"
        )

    config = BridgeConfig(
        remotive=remotive,
        kuksa=kuksa,
        options=options,
        to_vss=tuple(to_vss),
        to_can=tuple(to_can),
    )
    return ConfigLoadResult(config=config, skipped=tuple(skipped_vss + skipped_can))


def load_config(path: Path, *, default_allow_add: bool = DEFAULT_ALLOW_ADD) -> ConfigLoadResult:
    try:
        text = Path(path).read_text()
    except FileNotFoundError as exc:
        raise ConfigError(f"mapping file not found: {path}") from exc
    except OSError as exc:
        raise ConfigError(f"could not read mapping file {path}: {exc}") from exc
    return load_config_text(text, default_allow_add=default_allow_add)
