from pathlib import Path

from kx_vss_bridge.config import load_config

EXAMPLE = Path(__file__).resolve().parent.parent / "mapping.example.yaml"


def test_shipped_example_is_valid():
    """The example is run in the README, so it must actually parse."""
    result = load_config(EXAMPLE)
    assert not result.skipped
    assert result.config.to_vss
    assert result.config.to_can


def test_example_enum_labels_survive_yaml():
    """Guards the Norway problem in the file operators copy from."""
    mapping = next(
        m for m in load_config(EXAMPLE).config.to_vss
        if m.vss == "Vehicle.LowVoltageSystemState"
    )
    assert mapping.transform.enum_map[2] == "OFF"
    assert mapping.transform.enum_map[4] == "ON"
