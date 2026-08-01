# Copyright (c) 2024-present Kaizenics Systems GmbH.
# All rights reserved.
#
# This source code is proprietary and confidential.
# Unauthorized copying, modification, or distribution is strictly prohibited.
#
# https://kaizenics.com

"""VC — vehicle computer.

Consumes VSS_VehicleState (BCM) and provides VC_To_HMI (TelltaleId, ChimeId).
All decision logic lives in vc.control so it is testable without a broker.
"""

from __future__ import annotations

import asyncio

import structlog
from remotivelabs.broker import BrokerClient, Frame
from remotivelabs.topology.behavioral_model import BehavioralModel
from remotivelabs.topology.cli.behavioral_model import BehavioralModelArgs
from remotivelabs.topology.namespaces import filters
from remotivelabs.topology.namespaces.can import CanNamespace, RestbusConfig

from .control import decide
from .log import configure_logging

logger = structlog.get_logger(__name__)

ECU_NAME = "VC"
VEHICLE_CAN_NS = "VC-VehicleCAN"


def _sig(frame: Frame, name: str) -> int:
    """Frame signals are keyed 'Frame.Signal' on some broker versions, 'Signal' on others."""
    return int(frame.signals.get(f"{frame.name}.{name}", frame.signals.get(name, 0)))


class VC:
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
                self.vehicle_can.create_input_handler([filters.FrameFilter("VSS_VehicleState")], self.on_vehicle_state),
            ],
        )
        self._last: tuple[int, int] | None = None

    async def __aenter__(self):
        await self._broker_client.connect()
        await self.bm.start()
        await self.vehicle_can.restbus.update_signals(
            ("VC_To_HMI.TelltaleId", 0),
            ("VC_To_HMI.ChimeId", 0),
        )
        logger.info("VC online — VSS_VehicleState -> VC_To_HMI")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.bm.stop()
        await self._broker_client.disconnect()

    def __await__(self):
        return self.bm.run_forever().__await__()

    async def on_vehicle_state(self, frame: Frame) -> None:
        child = _sig(frame, "Vehicle_Cabin_ChildPresence_IsDetected")
        lv_state = _sig(frame, "Vehicle_LowVoltageSystemState")
        hazard = _sig(frame, "Vehicle_Body_Lights_Hazard_IsSignaling")
        horn = _sig(frame, "Vehicle_Body_Horn_IsActive")

        telltale, chime = decide(child, lv_state, hazard)
        if (telltale, chime) == self._last:
            return
        self._last = (telltale, chime)

        await self.vehicle_can.restbus.update_signals(
            ("VC_To_HMI.TelltaleId", telltale),
            ("VC_To_HMI.ChimeId", chime),
        )
        logger.info(
            "VC: HMI output updated",
            child=child,
            lv_state=lv_state,
            hazard=hazard,
            horn=horn,
            TelltaleId=telltale,
            ChimeId=chime,
        )


async def main(avp: BehavioralModelArgs) -> None:
    async with VC(avp) as ecu:
        await ecu


if __name__ == "__main__":
    args = BehavioralModelArgs.parse()
    configure_logging(args.loglevel)
    asyncio.run(main(args))
