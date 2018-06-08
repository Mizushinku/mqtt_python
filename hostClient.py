# -*- coding: utf-8 -*-

import paho.mqtt.client as mqtt
import time, signal, sys, threading
import db_Handler, ChatRoom, Record
import identifier as idf
import importlib

#importlib.reload(sys)
#sys.setdefaultencoding('utf-8')

client = None

mqtt_loop = False

db = db_Handler.DBHandler()
db.connect()

###################################

def on_message(client, userdata, message):
    topic = message.topic
    msg = str(message.payload.decode("utf-8"))
    thread = threading.Thread(target = hall, args = (topic, msg))
    thread.start()

def on_log(client, userdata, level, buf):
    print ("log : %s" % (buf))

###################################

def mqtt_client_thread():
    signal.signal(signal.SIGTERM, stop)
    #signal.signal(signal.SIGQUIT, stop)
    signal.signal(signal.SIGINT,  stop)  # Ctrl-C

    heartBeat = 0

    global mqtt_loop, client
    client_id = "gardenia"
    client = mqtt.Client(client_id,False)

    client.on_message = on_message
    client.on_log = on_log

    host_name = "140.116.82.52"
    client.connect(host_name,1883)

    topic = "IDF/+/+"

    client.subscribe(topic)

    mqtt_loop = True
    cnt = 0
    while mqtt_loop:
        client.loop()
        cnt += 1
        if cnt > 20:
            heartBeat += 1
            print ("HeartBeat = %d" % (heartBeat))
            try:
                client.reconnect()
            except:
                time.sleep(1)
            cnt = 0
    print("quit mqtt thread")
    client.disconnect()

###################################

def hall(topic, msg) :
    print("\n-------  into hall  -------\n")
    identifier = topic.split("/")[1]
    user = topic.split("/")[2]

    if   identifier == idf.FriendData :
        friendData(topic, user)
    elif identifier == idf.Initialize :
        initialize(topic, user)

def friendData(topic, user) :
    global db, client
    L = db.getFriendList(user)

def initialize(topic, user) :
    global db, client
    L = db.getInitInfo(user)
    topic_re = "%s/Re" % (topic)
    for R in L :
        msg = ("%s\t%s\t%s" % (R.code, R.roomName, R.type))
        print ("%s\n" % (msg))
        client.publish(topic_re,msg)
###################################

def stop(*args):
    global mqtt_loop
    mqtt_loop = False

###################################

if __name__ == "__main__":

    mqtt_client_thread()

    print("exit program")
    sys.exit(0)
