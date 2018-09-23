# -*- coding: utf-8 -*-

import paho.mqtt.client as mqtt
import time, signal, sys, threading
import db_Handler, ChatRoom, Record
import identifier as idf
import importlib
import datetime

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
    '''
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
    '''
    client.loop_forever()

###################################

def hall(topic, msg) :
    print("\n-------  into hall  -------\n")
    identifier = topic.split("/")[1]
    user = topic.split("/")[2]

    if   identifier == idf.FriendIcon :
        friendIcon(topic, user, msg)
    elif identifier == idf.Initialize :
        initialize(topic, user)
    elif identifier == idf.AddFriend :
        addFriend(topic, user, msg)
    elif identifier == idf.AddGroup :
        addGroup(topic, user, msg)
    elif identifier == idf.DeleteFriend :
        deleteFriend(topic, user, msg)
    elif identifier == idf.WithdrawFromGroup :
        withdrawFromGroup(topic, user, msg)
    elif identifier == idf.SendMessage :
        sendMessage(topic, user, msg)
    elif identifier == idf.GetRecord :
        getRecord(topic, user, msg)
    elif identifier == idf.Login :
        login(topic, user, msg)
    elif identifier == idf.AddFriendNotification :
        addFriendNotification(topic, user)

def login(topic, user, msg) :
    global db, client
    result = db.login(msg)
    if result == True :
        msg = "True," + msg
    else :
        msg = "False"
    topic_re = "%s/Re" % (topic)
    client.publish(topic_re,msg,2,False)

def friendIcon(topic, user, msg) :
    global db, client
    ID = msg.split(":")[1]
    img = db.getImage(ID)
    topic_re = "%s/Re" % (topic)
    topic_re = topic_re.replace("FriendIcon","FriendIcon," + msg)
    client.publish(topic_re,img,2,False)

def initialize(topic, user) :
    global db, client
    L = db.getInitInfo(user)
    topic_re = "%s/Re" % (topic)
    msg = ""
    for i in range(0,len(L)) :
        R = L[i]
        Rmsg = db.getLastMSG(R.code)
        Rmsg_Date = db.getLastMSGTime(R.code)
        if i == 0 :
            msg += ("%s\t%s\t%s\t%s\t%s\t%s" % (R.code, R.roomName, R.ID, R.type, Rmsg, Rmsg_Date))
        else :
            msg += (",%s\t%s\t%s\t%s\t%s\t%s" % (R.code, R.roomName, R.ID, R.type, Rmsg, Rmsg_Date))
        i = i + 1
    client.publish(topic_re,msg,2,False)

def addFriend(topic, user, friend) :
    global db, client
    result = db.addFriend(user,friend)
    topic_re = "%s/Re" % (topic)
    if result == True :
        last = db.getLast(user,"F")
        name = db.getName(friend)
        msg = "true/%s/%s/%s" % (name,friend,last)
        client.publish(topic_re,msg,2,False)
        
        topic_re = topic_re.replace(user,friend)
        name = db.getName(user)
        msg = "true/%s/%s/%s" % (name,user,last)
        client.publish(topic_re,msg,2,False)
    else :
        client.publish(topic_re,"false")

def deleteFriend(topic, user, msg) :
    global db, client
    friendID = msg.split("/")[0]
    code = msg.split("/")[1]
    result = db.deleteFriend(user, friendID, code)

    topic_re = "%s/Re" % (topic)
    if result == True :
        client.publish(topic_re,"true")
    else :
        client.publish(topic_re,"false")

def addGroup(topic, user, member_str) :
    global db, client
    L = member_str.split("\t")
    groupName = L[0]
    L.remove(L[0])
    result = db.createChatRoom(L,"G",groupName)
    topic_re = "%s/Re" % (topic)
    if result == True :
        last = db.getLast(user,"G")
        msg = "true/%s" % (last)
        client.publish(topic_re,msg)
    else :
        client.publish(topic_re,"false")

def withdrawFromGroup(topic, user, code) :
    global db, client
    result = db.withdrawFromGroup(user, code)
    topic_re = "%s/Re" % (topic)
    if result == True :
        client.publish(topic_re,"true")
    else :
        client.publish(topic_re,"false")

def sendMessage(topic, user, msg) :
    global db, client
    topic_splitLine = topic.split("/")
    msg_splitLine = msg.split("\t")
    code = msg_splitLine[0]
    sender = msg_splitLine[1]
    text = msg_splitLine[2]
    t = db.storeRecord(code,sender,text)
    msg = msg + "\t" + datetime.datetime.strftime(t, '%Y-%m-%d %H:%M:%S')
    receiver = db.getReceiverList(code)
    for R in receiver:
        topic_re = "%s/Re" % (topic_splitLine[0] + "/" + topic_splitLine[1] + "/" + R)
        client.publish(topic_re,msg)

def getRecord(topic, user, code) :
    global db, client
    L = db.getRecord(code)
    topic_re = "%s/Re" % (topic)
    msg = ""
    for i in range(0,len(L)) :
        R = L[i]
        if i == 0:
            msg += "%s\t%s\t%s" % (R.sender, R.MSG, R.time)
        else :
            msg += ",%s\t%s\t%s" % (R.sender, R.MSG, R.time)
        i = i + 1
    client.publish(topic_re,msg,2,False)
    #print(msg)

###################################

def addFriendNotification(topic, user) :
    global client
    topic_re = "%s/Re" % (topic)
    msg = ""
    client.publish(topic_re,msg,2,False)

###################################

def stop(*args) :
    global mqtt_loop
    mqtt_loop = False
    client.disconnect()
###################################

if __name__ == "__main__":

    mqtt_client_thread()

    print("exit program")
    sys.exit(0)
