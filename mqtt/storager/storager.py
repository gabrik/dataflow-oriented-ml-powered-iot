import configparser
import json
import logging
import pickle
from threading import Lock, Thread

from paho.mqtt import client as mqtt_client

logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.DEBUG)

def connect_mqtt(broker : str, port : str, client_id : str):
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

def subscribe(client: mqtt_client, topic_in : str, file):
    def on_message(client, userdata, msg):
        msg = json.loads(msg.payload)
        if "shutdown" in msg:
            file.close()
            client.disconnect()
        else:
            # del msg["timestamp"]
            file.write(json.dumps(msg)+"\n")
            file.flush()

    client.subscribe(topic_in)
    client.on_message = on_message

def run(broker : str, port : str, client_id : str, topic_in : str, file):

    client = connect_mqtt(broker, port, client_id)

    subscribe(client, topic_in, file)
    client.loop_forever()


if __name__ == '__main__':
    client_id = 'python-mqtt-batch-store'

    config = configparser.ConfigParser()
    config.read("./config.ini")

    broker = config.get("mosquitto", "host")
    port = int(config.get("mosquitto", "port"))
    topic_in = config.get("comparator", "results")
    file_name = config.get("storager", "file_name")
    file = open(file_name, "w")

    run(broker, port, client_id, topic_in, file)
