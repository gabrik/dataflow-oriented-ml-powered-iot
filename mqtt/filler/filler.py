import configparser
import datetime
import json
import logging
import time
from threading import Lock, Thread

import numpy as np
import pandas as pd
from paho.mqtt import client as mqtt_client

logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.DEBUG)

#Change to ordered messages
class DataHandler():

    def __init__(self, client : mqtt_client, topic_storage : str, topic_analisis : str, topic_ml : str, buffer_time : float, step_time : float):

        self.buffer = {}
        self.end_date = {}
        self.client = client
        self.topic_storage = topic_storage
        self.topic_analisis = topic_analisis
        self.topic_ml = topic_ml
        self.buffer_time = datetime.timedelta(minutes=buffer_time)
        self.step_time = datetime.timedelta(minutes=step_time)
        self.numerical_columns = {"temperature", "humidity", "pressure", "illuminance", "windspeed", "out_humidity",
                                    "out_temperature", "out_pressure", "precipitation"}
        self.bool_columns = {"contact", "occupancy"}
        self.n_samples = int(buffer_time / step_time)
        self.mutex = Lock()
        self.msg_queue = []

    def load_msg(self, msg):
        self.mutex.acquire()
        self.msg_queue.append(msg)
        self.mutex.release()

    def read_msg(self):
        self.mutex.acquire()
        if self.msg_queue:
            msg = self.msg_queue.pop(0)
            self.mutex.release()
            logging.debug('[Filler] Loaded message')
            if "shutdown" in msg:
                msg = self.client.publish(self.topic_analisis, json.dumps({"shutdown" : True, "timestamp" : str(datetime.datetime.now())}))
                msg = self.client.publish(self.topic_ml, json.dumps({"shutdown" : True, "timestamp" : str(datetime.datetime.now())}))
                msg = self.client.publish(self.topic_storage, json.dumps({"shutdown" : True, "timestamp" : str(datetime.datetime.now())}))
                self.client.disconnect()
                return False
            else:
                logging.debug("[Filler] Inserting data")
                self.insert_data(msg)
        else:
            self.mutex.release()
            time.sleep(0.00001)

        return True

    def insert_data(self, data : dict):
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
                self.send_data(house, data["records"], idx)
                self.end_date[house] = datetime.datetime.strptime(
                                    timestamp,
                                    "%Y-%m-%d %H:%M:%S") + self.buffer_time

    def send_data(self, house : str, records : dict, idx : int):
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
            msg = json.dumps(msg)

            self.client.publish(self.topic_storage, msg)
            self.client.publish(self.topic_analisis, msg)
            self.client.publish(self.topic_ml, msg)
            logging.debug(f"[Filler] Sent messages on: {self.topic_ml} {self.topic_analisis} {self.topic_storage}")
            self.buffer[house] = { key : value[-1:] for key, value in self.buffer[house].items()}

        except Exception as e:
            for key in self.buffer[house]:
                logging.debug(key + " " + str(len(self.buffer[house][key])))
            logging.debug(str(e))

            self.buffer[house] = { key : value[-1:] for key, value in self.buffer[house].items()}


def connect_mqtt(broker : str, port : str, client_id : str):
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            logging.debug("Connected to MQTT Broker!")
        else:
            logging.debug("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)

    client.on_connect = on_connect
    client.connect(broker, port)
    return client

def subscribe(client: mqtt_client, topic_in : str, data_handler : DataHandler):
    def on_message(client, userdata, msg):
        logging.debug(f"[Filler] Received message from topic: {msg.topic}")
        msg = json.loads(msg.payload)
        data_handler.load_msg(msg)

    client.subscribe(topic_in)
    client.on_message = on_message


def fill_data(filler : DataHandler, a):
    loop = True
    while loop:
        loop = filler.read_msg()
    logging.error("[Filler] Loop Ended!!!")

def run(broker : str, port : str, client_id : str, topic_in : str, topic_storage : str, topic_analisis : str,  topic_ml : str, buffer_time : float, step : float):
    client = connect_mqtt(broker, port, client_id)

    data_handler = DataHandler(client, topic_storage, topic_analisis, topic_ml, buffer_time, step)
    filler_thread = Thread(target = fill_data, args=(data_handler, 1))
    filler_thread.start()

    subscribe(client, topic_in, data_handler)
    client.loop_forever()

    filler_thread.join()

if __name__ == '__main__':
    client_id = 'python-mqtt-filler'

    config = configparser.ConfigParser()
    config.read("./config.ini")

    broker = config.get("mosquitto", "host")
    port = int(config.get("mosquitto", "port"))
    topic_in = config.get("aggregator", "topic_out")
    topic_storage = config.get("filler", "topic_storage")
    topic_analisis = config.get("filler", "analisis")
    topic_ml = config.get("filler", "topic_ml")
    buffer_time = int(config.get("filler", "buffer"))
    step = int(config.get("house", "step"))

    run(broker, port, client_id, topic_in, topic_storage, topic_analisis, topic_ml, buffer_time, step)