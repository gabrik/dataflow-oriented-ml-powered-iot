import configparser
import datetime
import json
import logging
import time

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

def subscribe(client: mqtt_client, topic_in : str, topic_out : str, sync_topic : str, n_clients : int):
    shutdowns = set()
    connected = set()
    def on_message(client, userdata, msg):
        logging.debug(f"[Aggregator] Received message on: {msg.topic}")
        msg_decode = json.loads(msg.payload)
        msg_decode["timestamp"] = str(datetime.datetime.now()),
        if "shutdown" in msg_decode:
            shutdowns.add(msg_decode["house"])
            if len(shutdowns) == n_clients:
                msg = client.publish(topic_out, json.dumps({"shutdown" : True, "timestamp" : str(datetime.datetime.now())}))
                client.disconnect()
        elif "ready" in msg_decode:
            connected.add(msg_decode["house"])
            if len(connected) == n_clients:
                time.sleep(1)
                client.publish(sync_topic, json.dumps({"start" : True, "timestamp" : str(datetime.datetime.now())}))
                logging.debug(f"[Aggregator] Sent ready message on: {sync_topic}")
        else:
            result = client.publish(topic_out, json.dumps(msg_decode))
            logging.debug(f"[Aggregator] Sent message on: {topic_out}")

            if result[0] != 0:
                    logging.debug(f"Failed to send message to topic {topic_out}")

    client.subscribe(topic_in)
    client.on_message = on_message

def run(broker : str, port : str, client_id : str, topic_in : str, topic_out : str, sync_topic : str, n_clients:int):
    client = connect_mqtt(broker, port, client_id)
    subscribe(client, topic_in, topic_out, sync_topic, n_clients)
    client.loop_forever()


if __name__ == '__main__':
    client_id = 'python-mqtt-aggregator'

    config = configparser.ConfigParser()
    config.read("./config.ini")

    broker = config.get("mosquitto", "host")
    port = int(config.get("mosquitto", "port"))
    topic_in = config.get("house", "topic_out")
    topic_out = config.get("aggregator", "topic_out")
    sync_topic= config.get("house", "topic_in")
    n_clients = int(config.get("general", "n_houses"))
    run(broker, port, client_id, topic_in, topic_out, sync_topic, n_clients)