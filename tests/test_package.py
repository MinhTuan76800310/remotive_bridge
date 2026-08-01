"""Packaging and container-contract checks.

These are deliberately static. They assert the shape of the deliverable — an
installable package and a container that runs it — without building an image or
starting anything, so they stay useful on a machine with no Docker.
"""

from __future__ import annotations

import importlib.metadata
import tomllib
from pathlib import Path

BRIDGE_ROOT = Path(__file__).resolve().parent.parent

# Verified against the installed SDKs; see the design doc's risk F8. A floating
# pin would silently change the subscribe and restbus semantics the bridge is
# built on.
REQUIRED_PINS = {
    "remotivelabs-broker": "remotivelabs-broker==0.9.1",
    "kuksa-client": "kuksa-client==0.5.2",
}


def _pyproject() -> dict:
    return tomllib.loads((BRIDGE_ROOT / "pyproject.toml").read_text())


def _dockerfile() -> str:
    return (BRIDGE_ROOT / "Dockerfile").read_text()


def test_package_has_version():
    assert importlib.metadata.version("kx-vss-bridge") == "0.1.0"


def test_sdk_versions_are_pinned_exactly():
    declared = _pyproject()["project"]["dependencies"]
    for name, expected in REQUIRED_PINS.items():
        matching = [d for d in declared if d.startswith(name)]
        assert matching == [expected], f"{name} must be pinned as {expected!r}"


def test_console_script_is_the_entrypoint():
    scripts = _pyproject()["project"]["scripts"]
    assert scripts["kx-vss-bridge"] == "kx_vss_bridge.__main__:main"


def test_dockerfile_runs_the_console_script_as_non_root():
    text = _dockerfile()
    assert 'ENTRYPOINT ["kx-vss-bridge"]' in text
    assert 'CMD ["--config", "/config/mapping.yaml"]' in text
    assert "USER 65532:65532" in text


def test_dockerfile_uses_python_312_slim():
    assert "FROM python:3.12-slim" in _dockerfile()


def test_image_excludes_neighbouring_packages_and_operator_data():
    """The bridge image must carry only the bridge.

    cpd-core is a separate deliverable and vss-vcar is a test rig; a real
    mapping.yaml can name internal signals. None belong in a published image.
    """
    ignored = (BRIDGE_ROOT / ".dockerignore").read_text().splitlines()
    for entry in (
        "cpd-core-only-1.0.0/",
        "cpd-core-only-1.0.0.tar.gz",
        "vss-vcar/",
        "mapping.yaml",
        "tests/",
        ".git/",
    ):
        assert entry in ignored, f".dockerignore must exclude {entry!r}"


def test_bridge_does_not_depend_on_the_management_repo():
    """The bridge is standalone.

    kx360v-management is a reference for how the broker is addressed, never an
    import. A dependency on docker/fastapi/neo4j here would mean the bridge had
    quietly grown into a sidecar.
    """
    declared = " ".join(_pyproject()["project"]["dependencies"]).lower()
    for forbidden in ("docker", "fastapi", "neo4j", "uvicorn"):
        assert forbidden not in declared
