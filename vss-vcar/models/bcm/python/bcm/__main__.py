# Copyright (c) 2024-present Kaizenics Systems GmbH.
# All rights reserved.
#
# This source code is proprietary and confidential.
# Unauthorized copying, modification, or distribution is strictly prohibited.
#
# https://kaizenics.com

"""BCM — body control module.

Provides VSS_VehicleState (child presence, low-voltage system state, hazard, horn)
and consumes VC_To_HMI: a CHILD_PRESENCE_ALERT chime makes it flash the hazards
and sound the horn, which is what makes Hazard.IsSignaling / Horn.IsActive move.

A built-in scenario loop drives child presence + ignition state so the car does
something on its own. Set BCM_SCENARIO=0 to keep the signals static and drive
them from the outside instead (e.g. `remotive broker signals set ...`).
"""

from __future__ import annotations

import asyncio
import os

import structlog
from remotivelabs.broker import BrokerClient, Frame
from remotivelabs.topology.behavioral_model import BehavioralModel
from remotivelabs.topology.cli.behavioral_model import BehavioralModelArgs
from remotivelabs.topology.namespaces import filters
from remotivelabs.topology.namespaces.can import CanNamespace, RestbusConfig

from .log import configure_logging

logger = structlog.get_logger(__name__)

ECU_NAME = "BCM"
VEHICLE_CAN_NS = "BCM-VehicleCAN"

LV_OFF = 2
LV_ON = 4
CHIME_CHILD_PRESENCE_ALERT = 1

# (hold_seconds, child_detected, low_voltage_system_state)
SCENARIO = [
    (10, 0, LV_ON),   # driving, nobody in the back
    (5, 0, LV_OFF),   # parked, ignition off
    (10, 1, LV_OFF),  # child left behind -> VC alerts -> BCM flashes hazards + horn
    (5, 0, LV_OFF),   # child removed -> alert clears
]


def _sig(frame: Frame, name: str) -> int:
    """Frame signals are keyed 'Frame.Signal' on some broker versions, 'Signal' on others."""
    return int(frame.signals.get(f"{frame.name}.{name}", frame.signals.get(name, 0)))


class BCM:
    def __init__(self, avp: BehavioralModelArgs) -> None:
        self._broker_client = BrokerClient(url=avp.url, auth=avp.auth)
        self.vehicle_can = CanNamespace(
            VEHICLE_CAN_NS,
            self._broker_client,
            restbus_configs=[
                RestbusConfig([filters.SenderFilter(ecu_name=ECU_NAME)], delay_multiplier=avp.delay_multiplier)
            ],
        )
        self.bm = BehavioralModel(
            ECU_NAME,
            namespaces=[self.vehicle_can],
            broker_client=self._broker_client,
            input_handlers=[
                self.vehicle_can.create_input_handler([filters.FrameFilter("VC_To_HMI")], self.on_hmi),
            ],
        )
        self._alerting = False

    async def __aenter__(self):
        await self._broker_client.connect()
        await self.bm.start()
        await self.vehicle_can.restbus.update_signals(
            ("VSS_VehicleState.Vehicle_Cabin_ChildPresence_IsDetected", 0),
            ("VSS_VehicleState.Vehicle_Body_Lights_Hazard_IsSignaling", 0),
            ("VSS_VehicleState.Vehicle_Body_Horn_IsActive", 0),
            ("VSS_VehicleState.Vehicle_LowVoltageSystemState", LV_ON),
        )
        logger.info("BCM online — providing VSS_VehicleState")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.bm.stop()
        await self._broker_client.disconnect()

    async def on_hmi(self, frame: Frame) -> None:
        """VC asked for a chime: escalate to hazards + horn. Chime cleared: stand down."""
        chime = _sig(frame, "ChimeId")
        alerting = chime == CHIME_CHILD_PRESENCE_ALERT
        if alerting == self._alerting:
            return
        self._alerting = alerting
        await self.vehicle_can.restbus.update_signals(
            ("VSS_VehicleState.Vehicle_Body_Lights_Hazard_IsSignaling", int(alerting)),
            ("VSS_VehicleState.Vehicle_Body_Horn_IsActive", int(alerting)),
        )
        if alerting:
            logger.warning("BCM: hazards + horn ON (child alert)")
        else:
            logger.info("BCM: hazards + horn off")

    async def run_scenario(self) -> None:
        while True:
            for hold_s, child, lv_state in SCENARIO:
                await self.vehicle_can.restbus.update_signals(
                    ("VSS_VehicleState.Vehicle_Cabin_ChildPresence_IsDetected", child),
                    ("VSS_VehicleState.Vehicle_LowVoltageSystemState", lv_state),
                )
                logger.info("BCM: vehicle state", child_detected=child, low_voltage_system_state=lv_state)
                await asyncio.sleep(hold_s)


async def main(avp: BehavioralModelArgs) -> None:
    async with BCM(avp) as ecu:
        if os.environ.get("BCM_SCENARIO", "1") == "1":
            await asyncio.gather(ecu.bm.run_forever(), ecu.run_scenario())
        else:
            await ecu.bm.run_forever()


if __name__ == "__main__":
    args = BehavioralModelArgs.parse()
    configure_logging(args.loglevel)
    asyncio.run(main(args))
