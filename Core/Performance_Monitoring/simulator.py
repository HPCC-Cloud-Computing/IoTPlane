import paho.mqtt.client as mqtt
import json
from random import randint
import time


broker_fog = '0.0.0.0'
clientMQTT = mqtt.Client('simulator')
clientMQTT.connect(broker_fog, 1883)


def on_disconnect(self, client, userdata, rc):
    if rc != 0:
        print("disconnect to Mosquitto.")


def on_connect(self, client, userdata, flags, rc):
    print("connected to Mosquitto")


clientMQTT.on_connect = on_connect
clientMQTT.on_disconnect = on_disconnect
while 1:
    for i in range(1, 10):
        message = {'temperature': randint(0, 100)}
        clientMQTT.publish('sensor_'+str(i), json.dumps(message))

    time.sleep(3)
