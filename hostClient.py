# -*- coding: utf-8 -*-

import paho.mqtt.client as mqtt
import time, signal, sys, threading
import db_Handler, ChatRoom, Record
import identifier as idf

reload(sys)
sys.setdefaultencoding('utf-8')

mqtt_loop = False

db = db_Handler.DBHandler()
db.connect()

###################################

def on_message(client, userdata, message):
    topic = message.topic
    msg = str(message.payload.decode("utf-8"))
    hall(topic,msg)

def on_log(client, userdata, level, buf):
    print("log : " + buf)

###################################

def mqtt_client_thread():
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGQUIT, stop)
    signal.signal(signal.SIGINT,  stop)  # Ctrl-C


    global mqtt_loop
    client_id = "gardenia"
    client = mqtt.Client(client_id,False)

    client.on_message = on_message
    #client.on_log = on_log

    host_name = "140.116.82.52"
    client.connect(host_name,1883)

    topic = "#"

    client.subscribe(topic)

    mqtt_loop = True
    cnt = 0
    while mqtt_loop:
        client.loop()
        cnt += 1
        if cnt > 20:
            try:
                client.reconnect()
            except:
                time.sleep(1)
            cnt = 0
    print("quit mqtt thread")
    client.disconnect()

###################################

def hall(topic, msg) :
    print("\ninto hall\n")
    identifier = topic.split("/")[0]
    user = topic.split("/")[1]
###################################

def stop(*args):
    global mqtt_loop
    mqtt_loop = False

###################################

if __name__ == "__main__":

    #mqtt_client_thread()
    print idf.FriendData

    print("exit program")
    sys.exit(0)
