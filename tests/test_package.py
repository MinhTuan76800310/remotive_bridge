"""Packaging and container-contract checks.

These are deliberately static. They assert the shape of the deliverable — an
installable package and a container that runs it — without building an image or
starting anything, so they stay useful on a machine with no Docker.
"""

from __future__ import annotations

import importlib.metadata
import os
import stat
import tomllib
from pathlib import Path

BRIDGE_ROOT = Path(__file__).resolve().parent.parent

# Where the image keeps the worked example. `run_remotive_vss_bridge.sh` names
# this path when the caller supplies no mapping, so the two must agree — that is
# what lets someone run the bridge with no repo checkout at all.
EXAMPLE_IN_IMAGE = "/usr/share/kx-vss-bridge/mapping.example.yaml"

# The published image. The run script pulls this every time; CI pushes to it.
IMAGE = "ghcr.io/minhtuan76800310/remotive_bridge"

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


def _run_script() -> str:
    return (BRIDGE_ROOT / "run_remotive_vss_bridge.sh").read_text()


def _workflow() -> str:
    return (BRIDGE_ROOT / ".github/workflows/docker-publish.yml").read_text()


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


# ── The published image and the script that runs it ──────────────────────────
#
# These three artefacts encode one contract between them: CI builds an image,
# the image carries a fallback mapping, and the script runs the image with that
# fallback. Each knows a literal the others must match, and a mismatch is only
# visible when someone else tries to run it — so it is asserted here instead.


def test_image_ships_the_example_mapping_outside_the_config_path():
    """Shipped so the image runs with no checkout; not at /config/mapping.yaml.

    If it were the default, an operator who forgot to mount their own mapping
    would get a bridge running happily against another vehicle's signal names
    instead of an error.
    """
    text = _dockerfile()
    assert f"COPY mapping.example.yaml {EXAMPLE_IN_IMAGE}" in text
    assert 'CMD ["--config", "/config/mapping.yaml"]' in text


def test_dockerignore_keeps_the_example_so_the_copy_can_find_it():
    ignored = (BRIDGE_ROOT / ".dockerignore").read_text().splitlines()
    assert "mapping.example.yaml" not in ignored
    # Still excluded: a real mapping names internal signals.
    assert "mapping.yaml" in ignored


def test_run_script_is_executable():
    mode = (BRIDGE_ROOT / "run_remotive_vss_bridge.sh").stat().st_mode
    assert mode & stat.S_IXUSR, "chmod +x — people run this by path, not via sh"


def test_run_script_always_pulls_the_latest_image_and_keeps_nothing():
    """`--rm` and an explicit pull are the whole point of handing this out.

    Without the pull, a stale local `latest` outlives every release; without
    `--rm`, repeated runs pile up dead containers under the same name.
    """
    text = _run_script()
    assert "--rm" in text
    assert f"{IMAGE}:latest" in text
    assert "pull" in text


def test_run_script_falls_back_to_the_mapping_shipped_in_the_image():
    """No argument must mean the image's own example, not a host path.

    The script has to work for someone who has only Docker and this one file.
    """
    text = _run_script()
    assert EXAMPLE_IN_IMAGE in text


def test_ci_builds_amd64_and_publishes_latest():
    text = _workflow()
    assert "linux/amd64" in text
    assert IMAGE.rsplit("/", 1)[-1] in text.lower()
    assert "value=latest" in text

