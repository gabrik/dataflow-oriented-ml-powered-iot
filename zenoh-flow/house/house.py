import configparser
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta
from threading import Lock, Thread

import numpy as np
import pandas as pd
import zenoh
from zenoh import config, Value

logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.DEBUG)

class Data():

    def __init__(self, loop : int, step : int):
        self.information = []
        self.recordings = {}
        self.out_measures = {"winddirection": None,
                            "out_pressure": None,
                            "precipitation": None,
                            "out_temperature": None,
                            "out_humidity": None,
                            "windspeed": None}
        self.cat_measuers = {"contact", "occupancy", "feedback"}
        self.mutex = Lock()
        self.last_timestamp = datetime.strptime("2019-03-01", "%Y-%m-%d")
        self.last_timestamp_outside = datetime.strptime("2019-03-01", "%Y-%m-%d")
        self.loop = loop
        self.loop_outside = timedelta(minutes=60)
        self.step = step
        self.stop = False

    def insert_data(self, data : dict):
        self.mutex.acquire()

        for key in data:
            if key != "timestamp":
                if key in self.recordings:
                    self.recordings[key].append((data["timestamp"], data[key]))
                else:
                    self.recordings[key] = [(data["timestamp"], data[key])]

        self.mutex.release()

    def collect_data(self):
        values = {"timestamps" : []}
        temp_timestamp = self.last_timestamp
        while temp_timestamp < self.last_timestamp + self.loop:
            values["timestamps"].append(temp_timestamp.strftime("%Y-%m-%d %H:%M:%S"))
            temp_timestamp += self.step

        if self.stop :
            return {}

        self.mutex.acquire()

        for key in self.recordings:
            values[key] = []
            raw_data = self.recordings[key]
            temp_timestamp = self.last_timestamp

            idx = 1  if temp_timestamp > raw_data[0][0] else 0

            if key not in self.out_measures:

                avg_values = []
                while temp_timestamp < self.last_timestamp + self.loop:
                    if idx >= len(raw_data):
                        if len(values[key]) >= 1:
                            values[key].append(values[key][-1])
                        else:
                            values[key].append(raw_data[idx - 1][1])
                        temp_timestamp += self.step

                    elif temp_timestamp < raw_data[idx][0] and temp_timestamp + self.step > raw_data[idx][0]:
                        avg_values.append(raw_data[idx][1])
                        idx += 1
                    elif temp_timestamp > raw_data[idx][0]:
                        idx += 1
                    else:
                        if key not in self.cat_measuers:
                            if len(avg_values) >= 1:
                                values[key].append(sum(avg_values)/len(avg_values))
                            else:                           #Do linear averaging or propagation depending on the amount of missing values
                                if raw_data[idx][0] - raw_data[idx-1][0] < self.step*2 and idx > 1:
                                    values[key].append(np.nan)
                                else:
                                    values[key].append(raw_data[idx-1][1])
                        else:
                            if len(avg_values) >= 1:        #Count the one with most appearences
                                counts = {}
                                for i in avg_values:
                                    counts[i] = counts.get(i, 0) +1
                                highest_count = (0,-1)
                                for i in counts:
                                    highest_count = (i, counts[i])  if counts[i] > highest_count[1] else highest_count
                                values[key].append(highest_count[0])
                            else:
                                values[key].append(raw_data[idx-1][1])
                        avg_values = []
                        temp_timestamp += self.step

                if idx < len(raw_data):
                    self.recordings[key] = raw_data[idx-1:]
                else:
                    self.recordings[key] = raw_data[-1:]
            else:
                while temp_timestamp < self.last_timestamp + self.loop:
                    if idx < len(raw_data) and raw_data[idx][0] < temp_timestamp:
                        values[key].append(raw_data[idx][1])
                        idx +=1
                    else:
                        values[key].append(raw_data[idx-1][1])
                    temp_timestamp += self.step

                if idx < len(raw_data):
                    self.recordings[key] = raw_data[idx-1:]
                else:
                    self.recordings[key] = raw_data[-1:]

        self.last_timestamp += self.loop
        self.mutex.release()
        try:
            df = pd.DataFrame(values)
            df = df.interpolate(method='linear', limit_direction='forward', axis=0)
            values = df.to_dict(orient="list")
            return values
        except:
            logging.debug("Error here, skipping")
            return {}


def connect_zenoh(broker, port):
    conf = zenoh.Config()

    # Set Connecting Client ID
    conf.insert_json5(zenoh.config.MODE_KEY, json.dumps("client"))
    locator = f"tcp/{broker}:{port}"
    conf.insert_json5(zenoh.config.CONNECT_KEY, json.dumps([locator]))
    zenoh.init_logger()
    session = zenoh.open(conf)
    return session

def publish(client, data_warehouse : Data, house_id : str, topic : str, loop_time : float):
    time.sleep(loop_time*3)
    logging.debug("[House] Starting messages")
    i = 0
    while True:
        time.sleep(loop_time)
        data = data_warehouse.collect_data()

        if len(data) > 0:
            msg = json.dumps({"house": house_id, "records": data, "timestamp": str(datetime.now())})
            # logging.debug(f"[House] Sending data {i}")
            client.put(topic, msg.encode("utf-8"))
            counter = 0
            i += 1
        else:
            counter += 1
            if counter == 5:
                logging.debug["[House] sending shutdown"]
                msg = json.dumps({"house" : house_id, "shutdown" : True, "timestamp" : str(datetime.now())})
                client.put(topic, msg.encode("utf-8"))
                time.sleep(2)
                client.close()
                break
    logging.debug("Done sending messages!!")


def run(data_warehouse : Data, house_id : str, broker : str, port, topic : str, loop_time):
    client = connect_zenoh(broker, port)
    publish(client, data_warehouse, house_id, topic, loop_time)

def read_file(file_name, data_warehouse, time_speed):
    start_time = datetime.strptime("2019-03-01", "%Y-%m-%d")

    file = open(file_name)
    file.readline()
    while True:
        data = file.readline()
        data = data.split(",")
        previous_time = datetime.strptime(data[0].split("+")[0].split(".")[0], "%Y-%m-%d %H:%M:%S")
        if previous_time > start_time:
            break

    for line in file:
        data = line.split(",")
        try:
            new_time = datetime.strptime(data[0].split("+")[0], "%Y-%m-%d %H:%M:%S.%f")
        except:
            new_time = datetime.strptime(data[0].split("+")[0].split(".")[0], "%Y-%m-%d %H:%M:%S")

        delta = (new_time - previous_time).total_seconds()

        time.sleep(delta*time_speed)
        if delta < 0:
            print("Error")
            break
        try:
            data = json.loads(re.sub('"{2}', '"', ",".join(data[1:])[1:-2]))
        except:
            print(re.sub('"{2}', '"', ",".join(data[1:])[1:-2]))
            break

        data["timestamp"] = new_time

        if "description" in data:
            del data["description"]
            data["out_pressure"] = data.pop("pressure")
            data["out_humidity"] = data.pop("humidity")
            data["out_temperature"] = data.pop("temperature")
            for key in list(data.keys()):
                if data[key] == None:
                    del data[key]
            data_warehouse.insert_data(data)

        elif "temperature" in data:
            keys = list(data.keys())
            for key in keys:
                if key != "temperature" and key != "humidity" and key != "pressure" and key != "timestamp":
                    del data[key]

            data_warehouse.insert_data(data)

        elif "contact" in data:
            keys = list(data.keys())
            for key in keys:
                if key != "contact" and key != "timestamp":
                    del data[key]
            data_warehouse.insert_data(data)

        elif "illuminance" in data:
            keys = list(data.keys())
            for key in keys:
                if key != "illuminance" and key != "occupancy" and key != "timestamp":
                    del data[key]

            data_warehouse.insert_data(data)

        elif "feedback" in data:
            del data["device"]

            data_warehouse.insert_data(data)

        previous_time = new_time


if __name__ == '__main__':
    house_id = os.environ["ID"].split("-")[-1]

    config = configparser.ConfigParser()
    config.read("./config.ini")

    broker = config.get("zenoh", "host")
    port = int(config.get("zenoh", "port"))
    topic = config.get("house", "topic_out")
    file_name = f"dataset/house{house_id}.csv"
    time_speed = float(config.get("house", "time_speed"))
    loop = timedelta(minutes=int(config.get("house", "loop")))
    step = timedelta(minutes=int(config.get("house", "step")))

    data_warehouse = Data(loop, step)
    zenoh_producer = Thread(target = run,args=(data_warehouse, house_id, broker, port, topic, loop.total_seconds()*time_speed))
    zenoh_producer.start()

    read_file(file_name, data_warehouse, time_speed)

    zenoh_producer.join()

