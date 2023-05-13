from zenoh_flow.interfaces import Operator
from zenoh_flow import Input, Output
from zenoh_flow.types import Context
from typing import Dict, Any
import logging
import datetime
import json


# This is just forwardning, not sure it is actually needed.
class Aggregator(Operator):
    def __init__(
        self,
        context: Context,
        configuration: Dict[str, Any],
        inputs: Dict[str, Input],
        outputs: Dict[str, Output],
    ):

        logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.DEBUG)
        self.output = outputs.get("filler", None)
        self.in_stream = inputs.get("house_data", None)

        self.shutdowns = []

        if self.in_stream is None:
            raise ValueError("No input 'house_data' found")
        if self.output is None:
            raise ValueError("No output 'filler' found")

    def finalize(self) -> None:
        return None

    async def iteration(self) -> None:
        data_msg = await self.in_stream.recv()
        # logging.debug("[Aggregator] Received data")
        msg_decode = json.loads(data_msg.data.decode("utf-8"))
        msg_decode["timestamp"] = str(datetime.datetime.now()),

        if "shutdown" in msg_decode:
            self.shutdowns.append(msg_decode["house"])
            shutdown_msg = json.dumps({"shutdown": True, "timestamp": str(datetime.datetime.now())}).encode("utf-8")
            await self.output.send(shutdown_msg)
        else:
            msg = json.dumps(msg_decode).encode("utf-8")
            await self.output.send(msg)

        # logging.debug("[Aggregator] Sent")
        return None


def register():
    return Aggregator
