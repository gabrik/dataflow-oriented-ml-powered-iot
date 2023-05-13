import configparser
import json
import logging
import os
import pathlib
import re
import time
from datetime import datetime, timedelta
from threading import Lock, Thread

import numpy as np
import pandas as pd
from paho.mqtt import client as mqtt_client

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

def connect_mqtt(broker, port, client_id):
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            logging.debug("Connected to MQTT Broker!")
        else:
            logging.debug("Failed to connect, return code %d\n", rc)
    # Set Connecting Client ID
    client = mqtt_client.Client(client_id)
    #client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

def publish(client : mqtt_client, data_warehouse : Data, house_id : str, topic : str, loop_time : float):
    time.sleep(loop_time*3)
    logging.debug(f"[House {house_id}] Starting messages on {topic}")
    counter = 0
    while True:
        time.sleep(loop_time)
        data = data_warehouse.collect_data()

        if len(data) > 0:
            msg =json.dumps({"house" : house_id, "records" : data, "timestamp" : str(datetime.now())})
            result = client.publish(topic, msg)

            if result[0] != 0:
                logging.debug(f"Failed to send message to topic {topic}")
            counter = 0
        else:
            counter += 1
            if counter == 5:
                msg = json.dumps({"house" : house_id, "shutdown" : True, "timestamp" : str(datetime.now())})
                msg = client.publish(topic, msg)
                client.disconnect()
                break
    client.disconnect()

def subscribe(client: mqtt_client, data_warehouse : Data, house_id : str,
        subcribe_topic, publish_topic : str, loop_time, file, previous_time, read_thread, publish_thread):
    def on_message(client, userdata, message):
        msg = json.loads(message.payload)

        if msg["start"]:
            read_thread.start()
            publish_thread.start()

    client.subscribe(subcribe_topic)
    client.on_message = on_message

def run(data_warehouse : Data, house_id : str, broker : str, port, client_id : str,
         subscribe_topic: str, publish_topic : str, loop_time, file, previous_time):
    client = connect_mqtt(broker, port, client_id)

    msg = json.dumps({"house" : house_id, "ready" : True, "timestamp" : str(datetime.now())})
    read_thread = Thread(target = read_file, args=(file, data_warehouse, time_speed, previous_time))
    publish_thread = Thread(target = publish, args=(client, data_warehouse, house_id, publish_topic, loop_time))

    subscribe(client, data_warehouse, house_id, subscribe_topic, publish_topic, loop_time, file, previous_time, read_thread, publish_thread)
    client.publish(publish_topic, msg)
    client.loop_forever()

    read_thread.join()
    publish_thread.join()

def read_file(file, data_warehouse, time_speed, previous_time):
    for line in file:

        data = line.split(",")
        try:
            new_time = datetime.strptime(data[0].split("+")[0], "%Y-%m-%d %H:%M:%S.%f")
        except:
            new_time = datetime.strptime(data[0].split("+")[0].split(".")[0], "%Y-%m-%d %H:%M:%S")

        delta = (new_time - previous_time).total_seconds()

        time.sleep(delta*time_speed)

        data = json.loads(re.sub('"{2}', '"', ",".join(data[1:])[1:-2]))


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

    data_warehouse.stop = True

if __name__ == '__main__':
    house_id = os.environ["ID"].split("-")[-1]
    client_id = f'python-mqtt-house-{house_id}'

    config = configparser.ConfigParser()
    config.read("./config.ini")

    broker = config.get("mosquitto", "host")
    port = int(config.get("mosquitto", "port"))
    subscribe_topic = config.get("house", "topic_in")
    publish_topic = config.get("house", "topic_out")
    file_name = pathlib.Path(config.get("house", "dataset_folder")) / f"house{house_id}.csv"
    time_speed = float(config.get("house", "time_speed"))
    loop = timedelta(minutes=int(config.get("house", "loop")))
    step = timedelta(minutes=int(config.get("house", "step")))

    start_time = datetime.strptime("2019-03-01", "%Y-%m-%d")

    file = open(file_name)
    file.readline()
    while True:
        data = file.readline()
        data = data.split(",")
        previous_time = datetime.strptime(data[0].split("+")[0].split(".")[0], "%Y-%m-%d %H:%M:%S")
        if previous_time > start_time:
            break

    data_warehouse = Data(loop, step)
    time.sleep(1)

    run(data_warehouse, house_id, broker, port, client_id, subscribe_topic, publish_topic,
        loop.total_seconds()*time_speed, file, previous_time)

    logging.debug(f"Shutting down house {house_id}")
