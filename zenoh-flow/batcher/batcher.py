from zenoh_flow.interfaces import Operator
from zenoh_flow import Input, Output
from zenoh_flow.types import Context
from typing import Dict, Any
import datetime
import json
import logging
import asyncio


DEFAULT_BUFFER_SIZE = 10000

# This is just forwardning, not sure it is actually needed.
class Batcher(Operator):
    def __init__(
        self,
        context: Context,
        configuration: Dict[str, Any],
        inputs: Dict[str, Input],
        outputs: Dict[str, Output],
    ):
        logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.DEBUG)
        self.configuration = configuration if configuration is not None else {}
        self.train_output = outputs.get("train", None)
        self.test_output = outputs.get("test", None)
        self.store_output = outputs.get("store", None)
        self.in_stream = inputs.get("batch", None)

        if self.in_stream is None:
            raise ValueError("No input 'batch' found")
        if self.train_output is None:
            raise ValueError("No output 'train' found")
        if self.test_output is None:
            raise ValueError("No output 'test' found")
        if self.store_output is None:
            raise ValueError("No output 'store' found")

        self.buffer = [[], []]
        self.buffer_size = self.configuration.get("buffer_size", DEFAULT_BUFFER_SIZE)

        self.mutex = asyncio.Lock()

    async def insert_data(self, data: list):
        await self.mutex.acquire()
        self.buffer[0] += data[0]
        self.buffer[1] += data[1]
        if len(self.buffer[0]) > self.buffer_size:
            await self.send_data()
            logging.debug("[Batcher] sent data")

        self.mutex.release()

    async def send_data(self):
        msg = {"data": self.buffer, "timestamp": str(datetime.datetime.now())}
        await self.train_output.send(json.dumps(msg).encode("utf-8"))
        await self.test_output.send(json.dumps(msg).encode("utf-8"))
        msg["data"] = self.buffer[0]
        await self.test_output.send(json.dumps(msg).encode("utf-8"))
        self.buffer = [[], []]

    def finalize(self) -> None:
        return None

    async def iteration(self) -> None:
        data_msg = await self.in_stream.recv()
        payload = json.loads(data_msg.data.decode("utf-8"))
        logging.debug("[Batcher] received data")
        if "shutdown" in payload:
            shutdown_msg = json.dumps({"shutdown": True, "timestamp": str(datetime.datetime.now())}).encode("utf-8")
            await self.test_output.send(shutdown_msg)
            await self.train_output.send(shutdown_msg)
            logging.debug("[Batcher] sent shutdown")
        else:
            await self.insert_data(payload["data"])

        return None


def register():
    return Batcher
