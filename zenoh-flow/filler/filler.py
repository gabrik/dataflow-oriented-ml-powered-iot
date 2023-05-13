from zenoh_flow.interfaces import Operator
from zenoh_flow import Input, Output
from zenoh_flow.types import Context
from typing import Dict, Any

import asyncio
import numpy as np
import pandas as pd
import json
import logging
import datetime


DEFAULT_BUFFER_TIME = 60
DEFAULT_SLEEP_TIME = 1


# This is just forwardning, not sure it is actually needed.
class Filler(Operator):
    def __init__(
        self,
        context: Context,
        configuration: Dict[str, Any],
        inputs: Dict[str, Input],
        outputs: Dict[str, Output],
    ):

        logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.DEBUG)
        self.storage_output = outputs.get("storage", None)
        self.ml_output = outputs.get("preprocessor", None)
        self.analisys_output = outputs.get("analisis", None)
        self.in_stream = inputs.get("filler", None)

        if self.in_stream is None:
            raise ValueError("No input 'filler' found")
        if self.storage_output is None:
            raise ValueError("No output 'storage' found")
        if self.ml_output is None:
            raise ValueError("No output 'preprocessor' found")
        if self.analisys_output is None:
            raise ValueError("No output 'analisis' found")

        self.configuration = configuration if configuration is not None else {}

        buffer_time = self.configuration.get("buffer_time", DEFAULT_BUFFER_TIME)
        step_time = self.configuration.get("step_time", DEFAULT_SLEEP_TIME)

        self.buffer = {}
        self.end_date = {}
        self.buffer_time = datetime.timedelta(minutes=buffer_time)
        self.step_time = datetime.timedelta(minutes=step_time)
        self.numerical_columns = {"temperature", "humidity", "pressure", "illuminance", "windspeed", "out_humidity",
                                    "out_temperature", "out_pressure", "precipitation"}
        self.bool_columns = {"contact", "occupancy"}
        self.n_samples = int(buffer_time / step_time)
        self.mutex = asyncio.Lock()

    async def insert_data(self, data):
        await self.mutex.acquire()
        house = data["house"]
        if house not in self.end_date:
            self.end_date[house] = datetime.datetime.strptime(
                                    data["records"]["timestamps"][0],
                                    "%Y-%m-%d %H:%M:%S") + self.buffer_time
            self.buffer[house] = {}

        for idx, timestamp in enumerate(data["records"]["timestamps"]):
            if datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S") < self.end_date[house]:
                for key in data["records"]:
                    if key in self.buffer[house]:
                        self.buffer[house][key].append(data["records"][key][idx])
                    else:
                        self.buffer[house][key] = [data["records"][key][idx], data["records"][key][idx]]
            else:
                await self.send_data(house, data["records"], idx)
                self.end_date[house] = datetime.datetime.strptime(
                                    timestamp,
                                    "%Y-%m-%d %H:%M:%S") + self.buffer_time

        self.mutex.release()

    async def send_data(self, house : str, records : dict, idx : int):
        msg = {"house" : house, "data": {}}

        times = self.buffer[house]["timestamps"]
        #Filling missing values
        for key in self.buffer[house]:

            #Filling values that might have missed before a batch
            start_timestamp = datetime.datetime.strptime(times[0],
                    "%Y-%m-%d %H:%M:%S")
            next_timestamp = datetime.datetime.strptime(times[1],
                "%Y-%m-%d %H:%M:%S")
            corrected_data = [self.buffer[house][key][0]]
            while next_timestamp - start_timestamp > self.step_time:
                if key == "timestamps":
                    corrected_data.append((start_timestamp + self.step_time).strftime("%Y-%m-%d %H:%M:%S"))
                else:
                    if key in self.numerical_columns:
                        corrected_data.append(np.nan)
                    else:
                        corrected_data.append(self.buffer[house][key][0])

                start_timestamp += self.step_time

            corrected_data += self.buffer[house][key][1:]

            #Filling values that might have missed after a batch
            start_timestamp = datetime.datetime.strptime(times[-1],
                    "%Y-%m-%d %H:%M:%S")
            next_timestamp = self.end_date[house]
            while next_timestamp - start_timestamp > self.step_time:
                if key == "timestamps":
                    corrected_data.append((start_timestamp + self.step_time).strftime("%Y-%m-%d %H:%M:%S"))
                else:
                    if key in self.numerical_columns:
                        corrected_data.append(np.nan)
                    else:
                        corrected_data.append(self.buffer[house][key][-1])
                start_timestamp += self.step_time

            corrected_data.append(records[key][idx])

            self.buffer[house][key] = corrected_data

        #Check if there is any missing values resulted from a new sensor being connected in the middle of batch.
        for key in self.buffer[house]:

            if len(self.buffer[house][key]) < self.n_samples +2:
                self.buffer[house][key] = ([self.buffer[house][key][0]]*(self.n_samples-len(self.buffer[house][key]) +2)) + self.buffer[house][key]
        try:

            df = pd.DataFrame(self.buffer[house])
            df = df.interpolate(method='linear', limit_direction='forward', axis=0)
            df = df.drop(index=[0, len(df)-1])
            self.buffer[house] = df.to_dict(orient="list")
            msg["data"] = self.buffer[house]
            msg["timestamp"] = str(datetime.datetime.now())

            # logging.debug(f"[Filler] seding data: {msg}")
            msg = json.dumps(msg).encode("utf-8")

            await self.storage_output.send(msg)
            await self.analisys_output.send(msg)
            await self.ml_output.send(msg)
            logging.debug("[Filler] sent data")
            self.buffer[house] = { key : value[-1:] for key, value in self.buffer[house].items()}

        except Exception as e:
            for key in self.buffer[house]:
                logging.debug(key + " " + str(len(self.buffer[house][key])))
            logging.debug(str(e))

            self.buffer[house] = { key : value[-1:] for key, value in self.buffer[house].items()}

    def finalize(self) -> None:
        return None

    async def iteration(self) -> None:
        data_msg = await self.in_stream.recv()
        # logging.debug("[Filler] received data")
        payload = json.loads(data_msg.data.decode("utf-8"))
        if "shutdown" in payload:
            shutdown_msg = json.dumps({"shutdown": True, "timestamp": str(datetime.datetime.now())}).encode("utf-8")
            await self.storage_output.send(shutdown_msg)
            await self.analisys_output.send(shutdown_msg)
            await self.ml_output.send(shutdown_msg)
            logging.debug("[Filler] sent shutdown")
        else:
            await self.insert_data(payload)

            return None


def register():
    return Filler
