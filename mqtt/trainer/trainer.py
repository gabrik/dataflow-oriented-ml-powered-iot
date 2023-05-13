import configparser
import datetime
import json
import logging
import os
import time
from threading import Lock, Thread

import numpy as np
from paho.mqtt import client as mqtt_client
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import matthews_corrcoef
from sklearn.model_selection import cross_val_score
from sklearn.neighbors import KNeighborsClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier

logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.DEBUG)


def predict(model, X, y):
    pred = model.predict(X)

    return matthews_corrcoef(y, pred)

class Trainer():

    def __init__(self, client : mqtt_client, topic_results : str, seed : int, id : int):
        self.client = client
        self.topic_results = topic_results
        self.X_train = []
        self.y_train = []

        np.random.seed(seed)
        rng = np.random.RandomState(seed)

        self.id = id
        if id == 1:
            self.model = SGDClassifier(loss='log_loss', penalty='l2', random_state=rng)
        elif id == 2:
            self.model = SGDClassifier(loss='hinge', penalty='l2', random_state=rng)
        elif id == 3:
            self.model = KNeighborsClassifier(n_neighbors=3)
        elif id == 4:
            self.model = KNeighborsClassifier(n_neighbors=5)
        elif id == 5:
            self.model = DecisionTreeClassifier(random_state=rng)
        elif id == 6:
            self.model = SVC(random_state=rng)
        elif id == 7:
            self.model = MLPClassifier(activation='relu', solver='adam', verbose=False, random_state=rng)
        else:
            self.model = RandomForestClassifier(random_state=rng)

        self.mutex = Lock()

        self.batch = 0
        self.msg_queue = []

        self.file = open(f"results/trainer-{id}.txt", "w+")
        self.file_data = open(f"results/trainer-data-{id}.txt", "w+")

    def read_msg(self):
        self.mutex.acquire()

        if self.msg_queue:
            logging.debug("[Trainer] Training...")
            msg = self.msg_queue.pop(0)
            self.mutex.release()

            if "shutdown" in msg:
                msg = self.client.publish(self.topic_results,
                        json.dumps({"shutdown" : True, "timestamp" : str(datetime.datetime.now())}))

                self.client.disconnect()
                return False

            self.file_data.write(f"{json.dumps(msg)}\n")
            self.file_data.flush()

            self.X_train += msg["data"][0]
            self.y_train += msg["data"][1]

            if 0 in self.y_train and 1 in self.y_train:
                self.train_models()
            self.batch += 1
            logging.debug("[Trainer] Training done")
        else:
            self.mutex.release()
            time.sleep(0.0001)
        return True

    def load_msg(self, msg : json):
        self.mutex.acquire()
        self.msg_queue.append(msg)
        self.mutex.release()

    def train_models(self):
        scores = cross_val_score(self.model, self.X_train, self.y_train, cv=5,scoring=predict, error_score=0, n_jobs=-1)
        logging.debug("[Trainer] Training scores {scores}")
        msg = {"timestamp" : str(datetime.datetime.now()), "mcc" : scores.mean(), "id" : self.id, "batch" : self.batch}
        self.client.publish(self.topic_results, json.dumps(msg))
        
        self.file.write(f"{json.dumps(msg)}\n")
        self.file.flush()
        
        logging.debug(f"[Trainer] Sent messages on: {self.topic_results}")

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

def subscribe(client: mqtt_client, topic_in : str, trainer : Trainer):

    def on_message(client, userdata, msg):
        logging.debug(f"[Trainer] Received message on: {msg.topic}")
        msg = json.loads(msg.payload)
        trainer.load_msg(msg)

    client.subscribe(topic_in)
    client.on_message = on_message

def run(broker : str, port : str, client_id : str, topic_data : str, topic_results : str, seed : int, trainer_id : int):

    client = connect_mqtt(broker, port, client_id)

    trainer = Trainer(client, topic_results, seed, trainer_id)

    train_thread = Thread(target = train_models, args=(trainer, 1))
    train_thread.start()

    subscribe(client, topic_data, trainer)

    client.loop_forever()

    train_thread.join()

def train_models(trainer : Trainer, a):

    loop = True
    while loop:
        loop = trainer.read_msg()

if __name__ == '__main__':
    trainer_id = int(os.environ["ID"].split("-")[-1])
    client_id = f'python-mqtt-trainer_{trainer_id+1}'
    config = configparser.ConfigParser()
    config.read("./config.ini")

    broker = config.get("mosquitto", "host")
    port = int(config.get("mosquitto", "port"))
    topic_data = config.get("batcher", "ml_train")
    topic_results = config.get("trainer", "results")

    seed = int(config.get("trainer", "seed"))

    run(broker, port, client_id, topic_data, topic_results, seed, trainer_id)
