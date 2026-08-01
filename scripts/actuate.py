"""Set a VSS actuation target — the way that tells you whether anyone heard.

    .venv/bin/python scripts/actuate.py Vehicle.Cabin.HMI.TelltaleId 2

`kuksa-client`'s `set_target_values()` writes the v1 *Target Value* field; this
script issues a v2 `Actuate` request. Since `1b30420` the bridge reads both, so
both reach CAN — but only `Actuate` reports back. Actuation is never buffered: it
reaches a live provider or it fails `UNAVAILABLE: Provider ... does not exist`. A
v1 write is stored whether or not a bridge exists, so its silent success is
indistinguishable from a dead bridge. `databroker-cli`'s `actuate` also writes v1,
hence this script. See docs/spike-f1-f6-findings.md (F11).
"""

from __future__ import annotations

import argparse
import asyncio
import sys

import grpc
from grpc.aio import AioRpcError
from kuksa.val.v2 import types_pb2 as types_v2
from kuksa.val.v2 import val_pb2 as v2
from kuksa_client.grpc import DataType
from kuksa_client.grpc.aio import VSSClient

# The `Value` oneof field must match the catalog's declared type exactly; the
# broker rejects int32 for a uint16 path with INVALID_ARGUMENT. Rather than make
# the caller know that, read the type and pick the field.
_FIELD_FOR_TYPE = {
    DataType.BOOLEAN: ("bool", lambda s: s.strip().lower() in ("1", "true", "t")),
    DataType.STRING: ("string", str),
    DataType.INT8: ("int32", int),
    DataType.INT16: ("int32", int),
    DataType.INT32: ("int32", int),
    DataType.INT64: ("int64", int),
    DataType.UINT8: ("uint32", int),
    DataType.UINT16: ("uint32", int),
    DataType.UINT32: ("uint32", int),
    DataType.UINT64: ("uint64", int),
    DataType.FLOAT: ("float", float),
    DataType.DOUBLE: ("double", float),
}


async def actuate(host: str, port: int, path: str, raw: str) -> int:
    async with VSSClient(host, port) as client:
        try:
            metadata = (await client.get_metadata([path]))[path]
        except Exception as exc:
            print(f"cannot read metadata for {path}: {exc}", file=sys.stderr)
            print(
                "Is the path in the databroker's catalog? The CPD overlay paths "
                "only exist on the stack started with cpd-min-overlay.json.",
                file=sys.stderr,
            )
            return 2

        field_and_cast = _FIELD_FOR_TYPE.get(metadata.data_type)
        if field_and_cast is None:
            print(
                f"{path} has type {metadata.data_type.name}, which this script "
                f"does not handle",
                file=sys.stderr,
            )
            return 2
        field, cast = field_and_cast

        try:
            value = cast(raw)
        except ValueError:
            print(
                f"{raw!r} is not a valid {metadata.data_type.name} for {path}",
                file=sys.stderr,
            )
            return 2

        request = v2.ActuateRequest(
            signal_id=types_v2.SignalID(path=path),
            value=types_v2.Value(**{field: value}),
        )
        try:
            await client.client_stub_v2.Actuate(
                request, metadata=client.generate_metadata_header(None)
            )
        except AioRpcError as exc:
            if exc.code() is grpc.StatusCode.UNAVAILABLE:
                # Actuation is never buffered: it reaches a live provider or is
                # refused. So this is the signature of "nothing is listening".
                print(f"no provider is registered for {path}", file=sys.stderr)
                print(
                    "The bridge is not running, or this path is not in its "
                    "to_can mapping. Check: curl -s localhost:8090/stats",
                    file=sys.stderr,
                )
                return 1
            print(f"Actuate rejected: {exc.code().name} — {exc.details()}", file=sys.stderr)
            return 1

    print(f"{path} = {value}  ({metadata.data_type.name}, delivered to provider)")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", help="VSS path, e.g. Vehicle.Cabin.HMI.TelltaleId")
    parser.add_argument("value", help="value to command")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=55557)
    args = parser.parse_args()
    sys.exit(asyncio.run(actuate(args.host, args.port, args.path, args.value)))


if __name__ == "__main__":
    main()
