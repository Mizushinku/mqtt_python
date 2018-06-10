# -*- coding: utf-8 -*-
import MySQLdb
import sys, time, hashlib
import ChatRoom, Record
import importlib
from MySQLdb import OperationalError

#importlib.reload(sys)
#sys.setdefaultencoding('utf-8')

class DBHandler:

    conn = None
    cursor = None

    def __init__(self) :
        self.host = "140.116.82.52"
        self.port = 3306
        self.user = "java"
        self.password = "ncku"
        self.database = "NCKULine"
        self.charset = "utf8"
        self.use_unicode = True

    def connect(self) :
        global conn, cursor
        conn = MySQLdb.connect( host = self.host,
                                port = self.port,
                                user = self.user,
                                passwd = self.password,
                                db = self.database,
                                charset = self.charset)
        cursor = conn.cursor()

    def re_connect(self) :
        global cursor
        try :
            sql = "SELECT null FROM foo"
            cursor.execute(sql)
        except OperationalError as e :
            if 'MySQL server has gone away' in str(e) :
                self.connect()
            else :
                raise e

    def confirmAccount(self, account) :
        self.re_connect()
        global cursor
        result = False
        
        sql = "SELECT studentID FROM students WHERE studentID = '%s'" % (account)
        cursor.execute(sql)
        if cursor.rowcount == 1:
            result = True

        return result

    def login(self, account) :
        self.re_connect()
        global cursor
        result = False

        sql = "SELECT studentID FROM students WHERE studentID = '%s'" % (account)
        cursor.execute(sql)
        if cursor.rowcount == 1:
            result = True

        return result
    
    def isFriend(self, user, friend) :
        self.re_connect()
        global cursor
        result = False
        if(self.confirmAccount(user) and self.confirmAccount(friend)) :
            sql = "SELECT null FROM friendMap WHERE user = '%s' AND friend = '%s'" % (user, friend)
            cursor.execute(sql)
            if cursor.rowcount > 0:
                result = True

        return result

    def addFriend(self, user, friend) :
        self.re_connect()
        global cursor, conn
        result = False
        if(self.confirmAccount(user) and self.confirmAccount(friend) and not(self.isFriend(user,friend)) and user != friend) :
            try :
                sql = "INSERT INTO friendMap(user, friend) VALUES('%s', '%s')" % (user, friend)
                cursor.execute(sql)
                conn.commit()
                sql = "INSERT INTO friendMap(user, friend) VALUES('%s', '%s')" % (friend, user)
                cursor.execute(sql)
                conn.commit()
                L = list((user, friend))
                if self.createChatRoom(L) :
                    result = True
            except :
                conn.rollback()

        return result

    def getFriendList(self, user) :
        self.re_connect()
        global cursor, conn
        friendList = list(())
        try :
            sql = "SELECT friend FROM friendMap WHERE user = '%s'" % (user)
            cursor.execute(sql)
            for i in range(0,cursor.rowcount) :
                row = cursor.fetchone()
                friendList.append(row[0])
        except :
            conn.rollback()

        return friendList

    def createChatRoom(self, memberList, Type = "F", groupName = None) :
        self.re_connect()
        global cursor, conn
        result = False
        memberList.sort()
        string = memberList[0]
        for i in range(1,len(memberList)) :
            string = "%s%s" % (string, memberList[i])
        if groupName is not None :
            string = "%s%s" % (string, groupName)
        code = self.MD5(string)
        if self.codeExist(code) :
            return result
        
        try :
            for i in range(0,len(memberList)) :
                cursor.execute("INSERT INTO RoomMap(code,GroupName,member,type) VALUES(%s,%s,%s,%s)", (code,groupName,memberList[i],Type))
                conn.commit()
            result = True
        except :
            conn.rollback()

        return result

    def getInitInfo(self, user) :
        self.re_connect()
        global cursor
        initInfo = list(())
        
        friendList = self.getFriendList(user)
        for i in range(0,len(friendList)) :
            friend = friendList[i]
            if self.cmp(user, friend) < 0 :
                string = "%s%s" % (user, friend)
            else :
                string = "%s%s" % (friend, user)
            code = self.MD5(string)
            initInfo.append(ChatRoom.ChatRoom(code,friend,"F"))

        sql = "SELECT code, GroupName FROM RoomMap WHERE GroupName IS NOT NULL AND member = '%s'" % (user)
        cursor.execute(sql)
        for i in range(0,cursor.rowcount) :
            row = cursor.fetchone()
            initInfo.append(ChatRoom.ChatRoom(row[0], row[1], "G"))

        return initInfo

    def getReceiverList(self, code) :
        self.re_connect()
        global cursor
        receiverList = list(())

        sql = "SELECT member FROM RoomMap WHERE code = '%s'" % (code)
        cursor.execute(sql)
        for i in range (0,cursor.rowcount) :
            row = cursor.fetchone()
            receiverList.append(row[0])

        return receiverList
    
    def storeRecord(self, code, sender, MSG) :
        self.re_connect()
        global cursor, conn
        try :
            sql = "INSERT INTO Record(code,sender,MSG) VALUES('%s','%s','%s')" % (code, sender, MSG)
            cursor.execute(sql)
            conn.commit()
        except :
            conn.rollback()

    def arrangeRecord(self, code) :
        self.re_connect()
        global cursor, conn
        try :
            sql = "SELECT null FROM Record WHERE code = '%s'" % (code)
            cursor.execute(sql)
            keep = 20
            if cursor.rowcount > keep :
                sql = "DELETE FROM Record WHERE code = '%s' ORDER BY time ASC LIMIT %d" % (code, cursor.rowcount - keep)
                cursor.execute(sql)
                conn.commit()

        except :
            conn.rollback()
    
    
    def getRecord(self, code) :
        self.re_connect()
        global cursor
        self.arrangeRecord(code)
        record = list(())
        sql = "SELECT sender, MSG FROM Record WHERE code = '%s'" % (code)
        cursor.execute(sql)

        for i in range(0,cursor.rowcount) :
            row = cursor.fetchone()
            record.append(Record.Record(row[0], row[1]))
        
        return record

    def getLast(self, user, Type) :
        self.re_connect()
        global cursor
        if Type == "F" :
            sql = "SELECT code FROM RoomMap WHERE member = '%s' AND Type = '%s' ORDER BY time DESC LIMIT 1" % (user,Type)
            cursor.execute(sql)
            row = cursor.fetchone()
            return row[0]
        elif Type == "G" :
            sql = "SELECT code, GroupName FROM RoomMap WHERE member = '%s' AND Type = '%s' ORDER BY time DESC LIMIT 1" % (user,Type)
            cursor.execute(sql)
            row = cursor.fetchone()
            return "%s/%s" % (row[0],row[1])

    def MD5(self, string) :
        encoder = hashlib.md5()
        encoder.update(string.encode('utf-8'))
        code = encoder.hexdigest().upper()
        return code

    def codeExist(self, code) :
        self.re_connect()
        global cursor
        result = False
        sql = "SELECT DISTINCT null FROM RoomMap WHERE code = '%s'" % (code)
        cursor.execute(sql)
        if cursor.rowcount > 0 :
            result = True

        return result

    def cmp(self, a, b) :
        return (a > b) - (a < b)

############################################################