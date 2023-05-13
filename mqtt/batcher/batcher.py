import configparser
import datetime
import json
import logging
import time
from threading import Lock, Thread

from paho.mqtt import client as mqtt_client

logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.DEBUG)

class Batcher():

    def __init__(self, client : mqtt_client, topic_ml_train : str,
            topic_ml_test : str, buffer_size : int, n_houses : int):
        self.client = client
        self.topic_ml_train = topic_ml_train
        self.topic_ml_test = topic_ml_test
        self.buffer = [[],[]]
        self.buffer_size = buffer_size
        self.n_houses = n_houses
        self.batch = 0
        self.mutex = Lock()
        self.msg_queue = []
        self.file = open("results/batcher.csv", "w+")

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
                msg = self.client.publish(self.topic_ml_test,
                        json.dumps({"shutdown" : True, "timestamp" : str(datetime.datetime.now())}))
                msg = self.client.publish(self.topic_ml_train,
                        json.dumps({"shutdown" : True, "timestamp" : str(datetime.datetime.now())}))
                self.client.disconnect()
                return False

            self.buffer[0] += msg["data"][0]
            self.buffer[1] += msg["data"][1]

            if len(self.buffer[0]) >= self.buffer_size: 
                self.send_data()
                self.batch +=1
        else:
            self.mutex.release()
            time.sleep(0.0001)
        return True

    def send_data(self):
        batch_x = self.buffer[0]
        batch_y = self.buffer[1]
            
        self.buffer = [[], []]

        msg = {"data" : [batch_x, batch_y], "timestamp" : str(datetime.datetime.now())}
        self.client.publish(self.topic_ml_train, json.dumps(msg))

        self.file.write(f"{json.dumps(msg)}\n")
        self.file.flush()


        msg["data"] = batch_x
        self.client.publish(self.topic_ml_test, json.dumps(msg))
        #logging.debug("Sending batch data")


        logging.debug(f"[Batcher] Sent messages on: {self.topic_ml_train} {self.topic_ml_test}")

def connect_mqtt(broker : str, port : str, client_id : str):
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            logging.debug("Connected to MQTT Broker!")
        else:
            logging.debug("Failed to connect, return code %d\n", rc)

    # Set Connecting Client ID
    client = mqtt_client.Client(client_id)

    client.on_connect = on_connect
    client.connect(broker, port)
    return client

def subscribe(client: mqtt_client, topic_in : str, batcher : Batcher):

    def on_message(client, userdata, msg):
        logging.debug(f"[Batcher] Received message from topic: {msg.topic}")
        msg = json.loads(msg.payload)
        batcher.load_msg(msg)

    client.subscribe(topic_in)
    client.on_message = on_message

def run(broker : str, port : str, client_id : str, topic_in : str,
        topic_ml_train : str, topic_ml_test : str, buffer_size: int, n_houses : int):

    client = connect_mqtt(broker, port, client_id)

    batcher = Batcher(client, topic_ml_train, topic_ml_test, buffer_size, n_houses)

    batch_thread = Thread(target = batch_data, args=(batcher, 1))
    batch_thread.start()

    subscribe(client, topic_in, batcher)
    client.loop_forever()

    batch_thread.join()

def batch_data(batcher : Batcher, a):
    loop = True
    while loop:
        loop = batcher.read_msg()
if __name__ == '__main__':
    client_id = 'python-mqtt-batcher'

    config = configparser.ConfigParser()
    config.read("./config.ini")

    broker = config.get("mosquitto", "host")
    port = int(config.get("mosquitto", "port"))
    topic_in = config.get("preprocessor", "topic_out")
    topic_ml_train = config.get("batcher", "ml_train")
    topic_ml_test= config.get("batcher", "ml_test")
    buffer_size = int(config.get("batcher", "buffer_size"))
    n_houses = int(config.get("general", "n_houses"))

    run(broker, port, client_id, topic_in, topic_ml_train, topic_ml_test, buffer_size, n_houses)
