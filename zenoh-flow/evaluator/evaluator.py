from zenoh_flow.interfaces import Operator
from zenoh_flow import Input, Output
from zenoh_flow.types import Context
from typing import Dict, Any
import json
import logging
import pickle
import asyncio
import sklearn_json as skljson
import datetime


# This is just forwardning, not sure it is actually needed.
class Evaluator(Operator):
    def __init__(
        self,
        context: Context,
        configuration: Dict[str, Any],
        inputs: Dict[str, Input],
        outputs: Dict[str, Output],
    ):

        logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.DEBUG)

        self.configuration = configuration if configuration is not None else {}

        self.output = outputs.get("predictions", None)
        self.in_model = inputs.get("test_model", None)
        self.in_data = inputs.get("test", None)

        if self.in_model is None:
            raise ValueError("No input 'test_model' found")
        if self.in_data is None:
            raise ValueError("No input 'test' found")
        if self.output is None:
            raise ValueError("No output 'predictions' found")

        self.clf = None
        self.buffer = []
        self.mutex = asyncio.Lock()

        # used for zenoh-flow inputs
        self.pending = []

    async def predict(self, data: list):
        await self.mutex.acquire()
        if self.clf is None:
            logging.debug("[Evaluator] No model yet, storing data")
            self.buffer += data
        else:
            logging.debug("[Evaluator] predict")
            if len(self.buffer) > 0:
                self.buffer += data
                y_pred = self.clf.predict(self.buffer)
                self.buffer = []
            else:
                y_pred = self.clf.predict(data)
            msg = {"data": y_pred.tolist(), "timestamp": str(datetime.datetime.now())}
            logging.debug(f"[Evaluator] done predictions {msg['data'][:5]} - sending...")
            await self.output.send(json.dumps(msg).encode("utf-8"))
        self.mutex.release()

    async def update_model(self, model):
        await self.mutex.acquire()
        logging.debug("[Evaluator] New model uploaded")
        self.clf = model
        self.mutex.release()

    def finalize(self) -> None:
        return None

    # Task magic for zenoh-flow
    async def wait_model(self):
        data_msg = await self.in_model.recv()
        return ("test_model", data_msg)

    async def wait_data(self):
        data_msg = await self.in_data.recv()
        return ("test", data_msg)

    def create_task_list(self):
        task_list = [] + self.pending

        if not any(t.get_name() == "test_model" for t in task_list):
            task_list.append(
                asyncio.create_task(self.wait_model(), name="test_model")
            )

        if not any(t.get_name() == "test" for t in task_list):
            task_list.append(asyncio.create_task(self.wait_data(), name="test"))
        return task_list
    #

    async def iteration(self) -> None:
        (done, pending) = await asyncio.wait(
            self.create_task_list(),
            return_when=asyncio.FIRST_COMPLETED,
        )

        self.pending = list(pending)

        for d in done:
            (who, data_msg) = d.result()

            if who == 'test_model':
                logging.debug("[Evaluator] received model")
                model = pickle.loads(data_msg.data)
                await self.update_model(model)
            elif who == 'test':
                payload = json.loads(data_msg.data.decode("utf-8"))
                if "shutdown" in payload:
                    logging.debug("[Evaluator] received shutdown")
                    return None
                logging.debug("[Evaluator] received data")
                await self.predict(payload["data"])
            else:
                pass
        return None


def register():
    return Evaluator
