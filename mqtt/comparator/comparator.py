import configparser
import datetime
import json
import logging
import pickle
import time
from threading import Lock, Thread

import numpy as np
from joblib import parallel_backend
from paho.mqtt import client as mqtt_client
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import SGDClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier

logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.DEBUG)

class Comparator():

    def __init__(self, client : mqtt_client, topic_ml_storage : str,
            topic_ml_test : str, topic_batcher : str, seed : int, n_classifiers : int):
        self.client = client
        self.topic_ml_test = topic_ml_test
        self.topic_ml_storage = topic_ml_storage
        self.topic_batcher = topic_batcher
        self.X_train = []
        self.y_train = []
        self.batch_data = {}
        self.best_performance = -1
        self.batch = -1
        self.stored_batch = 0
        self.results = {}
        np.random.seed(seed)
        self.rng = np.random.RandomState(seed)
        self.mutex = Lock()
        self.msg_queue = []
        self.shutdowns = 0
        self.n_classifiers = n_classifiers
        self.file = open("results/training_data.csv", "w")

    def read_msg(self):

        self.mutex.acquire()
        if self.msg_queue:

            msg = self.msg_queue.pop(0)
            self.mutex.release()

            if "shutdown" in msg:
                self.shutdowns += 1

                if self.shutdowns == self.n_classifiers:
                    msg = self.client.publish(self.topic_ml_storage,
                            json.dumps({"shutdown" : True, "timestamp" : str(datetime.datetime.now())}))

                    self.client.disconnect()
                    return False

                return True
            if msg["batch"] in self.results:
                self.results[msg["batch"]].append((msg["id"], msg["mcc"]))
            else:
                self.results[msg["batch"]] = [(msg["id"], msg["mcc"])]

            if len(self.results[msg["batch"]]) == self.n_classifiers:
                results = self.results.pop(msg["batch"])
                self.choose_best(results, msg["batch"])
        else:
            self.mutex.release()
            time.sleep(0.0001)


        return True

    def choose_best(self, results, batch):
        results = sorted(results, key=lambda x : (x[1], x[0]), reverse=True)
        best_id = results[0][0]
        best_mcc = results[0][1]
        
        while self.batch != batch:
            self.batch += 1
            x, y = self.batch_data[self.batch]

            for idx, row in enumerate(x):
                for value in row:
                    self.file.write(f"{value}, ")
                self.file.write(f"{y[idx]}\n")
            self.file.flush()

            self.X_train += x
            self.y_train += y


        msg = {"timestamp" : str(datetime.datetime.now()), "best_mcc" : best_mcc, "model" : f"{best_id}"}
        self.client.publish(self.topic_ml_storage, json.dumps(msg))

        if best_mcc > self.best_performance:
            if best_id == 1:
                model = SGDClassifier(loss='log_loss', penalty='l2', random_state=self.rng)
            elif best_id == 2:
                model = SGDClassifier(loss='hinge', penalty='l2', random_state=self.rng)
            elif best_id == 3:
                model = KNeighborsClassifier(n_neighbors=3)
            elif best_id == 4:
                model= KNeighborsClassifier(n_neighbors=5)
            elif best_id == 5:
                model = DecisionTreeClassifier(random_state=self.rng)
            elif best_id == 6:
                model = SVC(random_state=self.rng)
            elif best_id == 7:
                model = MLPClassifier(activation='relu', solver='adam', verbose=False, random_state=self.rng)
            else:
                model = RandomForestClassifier(random_state=self.rng)

            with parallel_backend('loky'):
                model.fit(self.X_train, self.y_train)

            self.client.publish(self.topic_ml_test, pickle.dumps(model))
            logging.debug(f"[Comparator] Sent message on: {self.topic_ml_test}")
            self.best_performance = best_mcc
            logging.debug(f"[Comparator] New best model with performance {best_mcc}, model is {model}")

    def store_data(self, msg):
        self.batch_data[self.stored_batch] = msg["data"]
        self.stored_batch += 1

    def load_msg(self, msg, topic):
        self.mutex.acquire()
        if topic == self.topic_batcher:
            if "shutdown" not in msg:
                self.store_data(msg)
        else:
            self.msg_queue.append(msg)

        self.mutex.release()

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

def subscribe(client: mqtt_client, topic_batcher : str, topic_trainer_results : str, comparator : Comparator):

    def on_message(client, userdata, message):
        logging.debug(f"[Comparator] Received message on: {message.topic}")
        msg = json.loads(message.payload)
        comparator.load_msg(msg, message.topic)

    client.subscribe(topic_batcher)
    client.subscribe(topic_trainer_results)
    client.on_message = on_message

def run(broker : str, port : str, client_id : str, topic_batcher : str,
        topic_ml_test : str, topic_trainer_results : str,
        topic_ml_storage :str, seed : int, n_classifiers : int):

    client = connect_mqtt(broker, port, client_id)

    comparator = Comparator(client, topic_ml_storage, topic_ml_test, topic_batcher, seed, n_classifiers)

    compare_thread = Thread(target = compare_models, args=(comparator, 1))
    compare_thread.start()

    subscribe(client, topic_batcher, topic_trainer_results, comparator)
    client.loop_forever()

    compare_thread.join()

def compare_models(comparator : Comparator, a):
    loop = True
    while loop:
        loop = comparator.read_msg()

if __name__ == '__main__':
    client_id = 'python-mqtt-comparator'

    config = configparser.ConfigParser()
    config.read("./config.ini")

    broker = config.get("mosquitto", "host")
    port = int(config.get("mosquitto", "port"))

    topic_batcher = config.get("batcher", "ml_train")
    topic_trainer_results = config.get("trainer", "results")

    topic_ml_test= config.get("comparator", "test_model")
    topic_ml_storage = config.get("comparator", "results")

    seed = int(config.get("trainer", "seed"))
    n_classifiers = int(config.get("general", "n_classifiers"))

    run(broker, port, client_id, topic_batcher, topic_ml_test, topic_trainer_results, topic_ml_storage, seed, n_classifiers)
