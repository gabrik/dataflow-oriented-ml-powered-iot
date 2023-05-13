import configparser
import datetime
import json
import logging
import time
from threading import Lock, Thread

import numpy as np
import pandas as pd
from paho.mqtt import client as mqtt_client
from sklearn.preprocessing import OneHotEncoder

logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.DEBUG)

class Preprocessor():

    def __init__(self, client : mqtt_client, topic_out : str):
        self.client = client
        self.topic_out = topic_out
        self.wind_dir_enc = OneHotEncoder(handle_unknown='ignore')
        self.wind_dir_enc.fit([["Este"], ["Oeste"], ["Norte"], ["Sul"], ["Nordeste"], ["Sudeste"], ["Noroeste"], ["Sudoeste"]])
        self.columns = {"temperature", "humidity", "pressure", "illuminance", "windspeed", "out_humidity",
                        "out_temperature", "out_pressure", "precipitation", "contact", "occupancy"}
        self.mutex = Lock()
        self.set_houses = set()
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
            if "shutdown" in msg:
                msg = self.client.publish(self.topic_out, json.dumps({"shutdown" : True, "timestamp" : str(datetime.datetime.now())}))
                self.client.disconnect()
                return False
            #logging.debug("Preprocessing")
            self.preprocess_and_send(msg)
        else:
            time.sleep(0.00001)
            self.mutex.release()

        return True

    def preprocess_and_send(self, entries : dict):
        if self.columns.issubset(set(entries["data"].keys())):
            '''Create a good rule for presence'''
            if "feedback" in entries["data"]:
                del entries["data"]["feedback"]

            entries["data"]["occupancy"] = [1 if example else 0
                                             for example in entries["data"]["occupancy"]]

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
                self.client.publish(self.topic_out, json.dumps({"data": [x, y], "timestamp" : str(datetime.datetime.now()), "house" : entries["house"]}))
                logging.debug(f"[Preprocessor] Sent messages on: {self.topic_out}")
                if len(self.set_houses) == 13:
                    logging.debug("All houses")
                else:
                    self.set_houses.add(entries["house"])
            except:
                logging.debug("Error of invalid size")

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

def subscribe(client: mqtt_client, topic_in : str, preprocessor : Preprocessor):
    def on_message(client, userdata, msg):
        logging.debug(f"[Preprocessor] Received message on: {msg.topic}")
        msg = json.loads(msg.payload)
        preprocessor.load_msg(msg)

    client.subscribe(topic_in)
    client.on_message = on_message

def preprocess_data(preprocessor : Preprocessor, a):
    loop = True
    while loop:
        loop = preprocessor.read_msg()

def run(broker : str, port : str, client_id : str, topic_in : str, topic_out : str):
    client = connect_mqtt(broker, port, client_id)
    preprocessor = Preprocessor(client, topic_out)

    preprocessor_thread = Thread(target = preprocess_data, args=(preprocessor, 1))
    preprocessor_thread.start()

    subscribe(client, topic_in, preprocessor)

    client.loop_forever()
    preprocessor_thread.join()

if __name__ == '__main__':
    client_id = 'python-mqtt-preprocessor'

    config = configparser.ConfigParser()
    config.read("./config.ini")

    broker = config.get("mosquitto", "host")
    port = int(config.get("mosquitto", "port"))
    topic_in = config.get("filler", "topic_ml")
    topic_out = config.get("preprocessor", "topic_out")

    run(broker, port, client_id, topic_in, topic_out)