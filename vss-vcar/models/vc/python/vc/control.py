# Copyright (c) 2024-present Kaizenics Systems GmbH.
# All rights reserved.
#
# This source code is proprietary and confidential.
# Unauthorized copying, modification, or distribution is strictly prohibited.
#
# https://kaizenics.com

"""VC decision table: VSS_VehicleState -> VC_To_HMI.

Pure functions, no broker. Run `python -m vc.control` for the self-check.
"""

from __future__ import annotations

# Vehicle.LowVoltageSystemState (VSS enum, mirrored in VehicleState.dbc VAL_ table)
LV_UNDEFINED = 0
LV_LOCK = 1
LV_OFF = 2
LV_ACC = 3
LV_ON = 4
LV_START = 5

# Vehicle is parked / unattended: the child-presence alert only escalates here.
LV_PARKED = frozenset({LV_LOCK, LV_OFF})

# Vehicle.Cabin.HMI.TelltaleId
TELLTALE_NONE = 0
TELLTALE_CHILD_PRESENCE = 1
TELLTALE_HAZARD = 2

# Vehicle.Cabin.HMI.ChimeId
CHIME_NONE = 0
CHIME_CHILD_PRESENCE_ALERT = 1


def decide(child_detected: int, lv_state: int, hazard_signaling: int) -> tuple[int, int]:
    """Map the vehicle state to (TelltaleId, ChimeId).

    Priority: unattended child > child on board > hazard reflection > idle.
    """
    if child_detected:
        if lv_state in LV_PARKED:
            return TELLTALE_CHILD_PRESENCE, CHIME_CHILD_PRESENCE_ALERT
        # Driver still present — indicate, do not chime.
        return TELLTALE_CHILD_PRESENCE, CHIME_NONE
    if hazard_signaling:
        return TELLTALE_HAZARD, CHIME_NONE
    return TELLTALE_NONE, CHIME_NONE


def _self_check() -> None:
    assert decide(1, LV_OFF, 0) == (TELLTALE_CHILD_PRESENCE, CHIME_CHILD_PRESENCE_ALERT)
    assert decide(1, LV_LOCK, 0) == (TELLTALE_CHILD_PRESENCE, CHIME_CHILD_PRESENCE_ALERT)
    assert decide(1, LV_ON, 0) == (TELLTALE_CHILD_PRESENCE, CHIME_NONE)
    assert decide(1, LV_START, 1) == (TELLTALE_CHILD_PRESENCE, CHIME_NONE)
    assert decide(0, LV_OFF, 1) == (TELLTALE_HAZARD, CHIME_NONE)
    assert decide(0, LV_ON, 0) == (TELLTALE_NONE, CHIME_NONE)
    # BCM raises hazard+horn off the chime; once the child is gone the loop must
    # settle back to NONE within two exchanges rather than oscillate.
    assert decide(0, LV_OFF, 1) == (TELLTALE_HAZARD, CHIME_NONE)
    assert decide(0, LV_OFF, 0) == (TELLTALE_NONE, CHIME_NONE)
    print("vc.control self-check OK")


if __name__ == "__main__":
    _self_check()
