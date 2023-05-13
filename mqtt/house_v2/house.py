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

logging.basicConfig(level=logging.DEBUG)

class Data():

    def __init__(self, loop, step, time):
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
        self.last_timestamp = time
        self.last_timestamp_outside = time
        self.loop = loop
        self.loop_outside = timedelta(minutes=60)
        self.step = step

    def insert_data(self, data : dict):        
        for key in data:
            if key != "timestamp":
                if key in self.recordings:
                    self.recordings[key].append((data["timestamp"], data[key]))
                else:
                    self.recordings[key] = [(data["timestamp"], data[key])]
    
    def collect_data(self):
        values = {"timestamps" : []}
        temp_timestamp = self.last_timestamp
        while temp_timestamp < self.last_timestamp + self.loop:
            values["timestamps"].append(temp_timestamp.strftime("%Y-%m-%d %H:%M:%S"))
            temp_timestamp += self.step
        
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

        if len(values) > 1:
            df = pd.DataFrame(values)
            df = df.interpolate(method='linear', limit_direction='forward', axis=0)
            values = df.to_dict(orient="list")
            return values
        else:
            #logging.debug("Error here, skipping")
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

def publish(client : mqtt_client, data_warehouse : Data, house_id : str, topic : str):

    data = data_warehouse.collect_data()

    if len(data) > 0:
        msg =json.dumps({"house" : house_id, "records" : data, "timestamp" : str(datetime.now())})
        result = client.publish(topic, msg)

        if result[0] != 0:
            logging.debug(f"Failed to send message to topic {topic}")

def read_files(houses_files, time_speed, current_time, loop, client, publish_topic):
    step = 1
    step_seconds = timedelta(seconds=1)
    stoped_houses = 0
    counter = 1
    while stoped_houses != len(houses_files):
        for idx, house in enumerate(houses_files):
            if not house:
                continue
            data_warehouse = house[1]
            file = house[0]
            previous_time = house[2]
            data = house[3]
            while previous_time < current_time:
                
                data["timestamp"] = previous_time

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

                line = file.readline() ## Continue later, check here
                if not line:
                    msg = json.dumps({"house" : idx+1, "shutdown" : True, "timestamp" : str(datetime.now())})
                    msg = client.publish(topic, msg)
                    stoped_houses += 1
                    houses_files[idx] = None
                    break
                
                data = line.split(",")

                try:
                    previous_time = datetime.strptime(data[0].split("+")[0], "%Y-%m-%d %H:%M:%S.%f")
                except:
                    previous_time = datetime.strptime(data[0].split("+")[0].split(".")[0], "%Y-%m-%d %H:%M:%S")
                
                data = json.loads(re.sub('"{2}', '"', ",".join(data[1:])[1:-2]))
            
            houses_files[idx][2] = previous_time
            houses_files[idx][3] = data

            if counter % loop == 0:
                publish(client, data_warehouse, idx+1, publish_topic)

        current_time += step_seconds
        counter += step
        time.sleep(1*time_speed)
            

if __name__ == '__main__':
    client_id = f'python-mqtt-houses'

    config = configparser.ConfigParser()
    config.read("./config.ini")

    broker = config.get("mosquitto", "host")
    port = int(config.get("mosquitto", "port"))
    publish_topic = config.get("house", "topic_out")

    n_houses = int(config.get("general", "n_houses"))
    time_speed = float(config.get("house", "time_speed"))
    loop = int(config.get("house", "loop"))
    loop_delta = timedelta(minutes=loop)
    step = timedelta(minutes=int(config.get("house", "step")))

    current_time = datetime.strptime("2019-03-01", "%Y-%m-%d")

    houses_files = []
    for house in range(1, n_houses+1):
        file_name = pathlib.Path(config.get("house", "dataset_folder")) / f"house{house}.csv"
        file = open(file_name)
        file.readline()
        while True:
            data = file.readline()
            data = data.split(",")
            previous_time = datetime.strptime(data[0].split("+")[0].split(".")[0], "%Y-%m-%d %H:%M:%S")
            if previous_time > current_time:
                break
        
        data_warehouse = Data(loop_delta, step, previous_time)
        data = json.loads(re.sub('"{2}', '"', ",".join(data[1:])[1:-2]))
        houses_files.append([file, data_warehouse, previous_time, data])



    client = connect_mqtt(broker, port, client_id)

    read_files(houses_files, time_speed, current_time, loop, client, publish_topic)
    client.disconnect()
    logging.debug(f"Shutting down houses")
    