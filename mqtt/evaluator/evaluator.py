import configparser
import datetime
import json
import logging
import pickle
import time
from threading import Lock, Thread

from paho.mqtt import client as mqtt_client

logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.DEBUG)

class Predictor():

    def __init__(self, client : mqtt_client, topic_predictions : str):
        self.client = client
        self.clf = None
        self.buffer = []
        self.topic_predictions = topic_predictions
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
            try:
                msg = json.loads(msg)
                if "shutdown" in msg:
                    self.client.disconnect()
                    return False

                self.predict(msg["data"])
            except:
                self.update_model(pickle.loads(msg))
        else:
            time.sleep(0.00001)
            self.mutex.release()

        return True

    def predict(self, data : list):
        if self.clf is None:
            self.buffer += data
        else:
            if len(self.buffer) > 0:
                self.buffer += data
                y_pred = self.clf.predict(self.buffer)
                self.buffer = []
            else:
                y_pred = self.clf.predict(data)
            #logging.debug(y_pred)
            msg = {"data" :y_pred.tolist(), "timestamp" : str(datetime.datetime.now())}
            self.client.publish(self.topic_predictions, json.dumps(msg))

    def update_model(self, model):
        self.clf = model

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

def subscribe(client: mqtt_client,  topic_in_model : str, topic_in_data : str, predictor : Predictor):

    def on_message(client, userdata, msg):
        predictor.load_msg(msg.payload)

    client.subscribe(topic_in_model)
    client.subscribe(topic_in_data)
    client.on_message = on_message

def predict_data(predictor : Predictor, a):
    loop = True
    while loop:
        loop = predictor.read_msg()

def run(broker : str, port : str, client_id : str, topic_in_model : str, topic_in_data : str, topic_predictions : str):

    client = connect_mqtt(broker, port, client_id)

    predictor = Predictor(client, topic_predictions)

    predictor_thread = Thread(target = predict_data, args=(predictor, 1))
    predictor_thread.start()

    subscribe(client, topic_in_model, topic_in_data, predictor)
    client.loop_forever()

    predictor_thread.join()

if __name__ == '__main__':
    client_id = 'python-mqtt-predictor'

    config = configparser.ConfigParser()
    config.read("./config.ini")

    broker = config.get("mosquitto", "host")
    port = int(config.get("mosquitto", "port"))
    topic_in_model = config.get("comparator", "test_model")
    topic_in_data = config.get("batcher", "ml_test")
    topic_predictions = config.get("evaluator", "topic_out")

    run(broker, port, client_id, topic_in_model, topic_in_data, topic_predictions)
