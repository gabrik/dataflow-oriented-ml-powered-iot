from zenoh_flow.interfaces import Operator
from zenoh_flow import Input, Output
from zenoh_flow.types import Context
from typing import Dict, Any
import asyncio
import datetime
import numpy as np
import pandas as pd
from sklearn.preprocessing import OneHotEncoder
import json
import logging


class Preprocessor(Operator):
    def __init__(
        self,
        context: Context,
        configuration: Dict[str, Any],
        inputs: Dict[str, Input],
        outputs: Dict[str, Output],
    ):
        logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.DEBUG)
        self.output = outputs.get("batch", None)
        self.in_stream = inputs.get("preprocessor", None)

        if self.in_stream is None:
            raise ValueError("No input 'preprocessor' found")
        if self.output is None:
            raise ValueError("No output 'batch' found")

        self.wind_dir_enc = OneHotEncoder(handle_unknown='ignore')
        self.wind_dir_enc.fit([["Este"], ["Oeste"], ["Norte"], ["Sul"], ["Nordeste"], ["Sudeste"], ["Noroeste"], ["Sudoeste"]])
        self.columns = {"temperature", "humidity", "pressure", "illuminance", "windspeed", "out_humidity",
                        "out_temperature", "out_pressure", "precipitation", "contact", "occupancy"}
        self.mutex = asyncio.Lock()

    def finalize(self) -> None:
        return None

    async def preprocess(self, entries : dict) -> dict:
        await self.mutex.acquire()
        # logging.debug("[Preprocessor] processing")
        if self.columns.issubset(set(entries["data"].keys())):
            '''Create a good rule for presence'''
            if "feedback" in entries["data"]:
                del entries["data"]["feedback"]

            entries["data"]["occupancy"] = [1 if example else 0 for example in entries["data"]["occupancy"]]

            del entries["data"]["timestamps"]

            entries["data"]["contact"] = [1 if example else 0 for example in entries["data"]["contact"]]

            wind_enc = self.wind_dir_enc.transform([[x] for x in entries["data"]["winddirection"]]).toarray()

            del entries["data"]["winddirection"]

            for example in wind_enc:
                for idx, cat in enumerate(self.wind_dir_enc.categories_[0]):
                    if cat in entries["data"]:
                        entries["data"][cat].append(example[idx])
                    else:
                        entries["data"][cat] = [example[idx]]
            try:
                order = ['occupancy','temperature', 'humidity', 'pressure', 'illuminance',
                            'contact', 'windspeed', 'out_pressure', 'out_humidity' ,'out_temperature',
                             'precipitation', 'Este', 'Nordeste', 'Noroeste', 'Norte', 'Oeste', 'Sudeste', 'Sudoeste', 'Sul']
                df = pd.DataFrame.from_dict(entries["data"])[order]
                df.dropna(inplace=True)

                train_data = df.values
                x = train_data[:,1:].tolist()
                y = train_data[:,0].tolist()
                msg = json.dumps({"data": [x, y], "timestamp": str(datetime.datetime.now())}).encode("utf-8")
                # logging.debug(f"[preprocessor ] sending : {msg}")
                self.mutex.release()
                return msg
            except Exception as e:
                logging.debug(f"[preprocessor ] Error: {e}")

        self.mutex.release()
        # logging.debug("[Preprocessor] done processing")
        return None

    async def iteration(self) -> None:
        data_msg = await self.in_stream.recv()
        logging.debug("[Preprocessor] received data")
        msg = json.loads(data_msg.data.decode("utf-8"))

        if "shutdown" in msg:
            shutdown_msg = json.dumps({"shutdown": True, "timestamp": str(datetime.datetime.now())}).encode("utf-8")
            await self.output.send(shutdown_msg)
            logging.debug("[Preprocessor] sent shutdown")
        else:
            reply = await self.preprocess(msg)
            if reply is not None:
                logging.debug("[Preprocessor] sending data")
                await self.output.send(reply)

        return None


def register():
    return Preprocessor
