# -*- coding: utf-8 -*-
#import MySQLdb
import pymysql
import sys, time, hashlib
from datetime import datetime
import ChatRoom, Record, Post
import importlib
import threading
#from MySQLdb import OperationalError
pymysql.install_as_MySQLdb()

#importlib.reload(sys)
#sys.setdefaultencoding('utf-8')

class DBHandler:


    def __init__(self, conn_in, cursor_in) :
        self.conn = conn_in
        self.cursor = cursor_in
        '''
        self.host = "140.116.82.52"
        self.port = 3306
        self.user = "java"
        self.password = "ncku"
        self.database = "NCKULine"
        self.charset = "utf8"
        self.use_unicode = True
        '''
    @staticmethod
    def connect() :
        conn = pymysql.connect( host = "140.116.82.52",
                                port = 3306,
                                user = "java",
                                passwd = "ncku",
                                db = "NCKULine",
                                charset = "utf8")
        #self.cursor = self.conn.cursor()
        return conn

    def re_connect(self) :
        # global cursor
        try :
            sql = "SELECT null FROM foo"
            self.cursor.execute(sql)
        except pymysql.err.OperationalError as e :
            if 'MySQL server has gone away' in str(e) :
                self.conn = DBHandler.connect()
                self.cursor = self.conn.cursor()
            else :
                raise e

    def confirmAccount(self, account) :
        self.re_connect()
        # global cursor
        result = False

        sql = "SELECT studentID FROM students WHERE studentID = %s"
        args = (account)
        self.cursor.execute(sql, args)
        if self.cursor.rowcount == 1:
            result = True

        return result

    def login(self, account) :
        self.re_connect()
        # global cursor
        result = False

        sql = "SELECT studentID FROM students WHERE studentID = %s"
        args = (account)
        self.cursor.execute(sql, args)
        if self.cursor.rowcount == 1:
            result = True

        return result

    def userVerify(self, userID, password) :
        self.re_connect()
        # global cursor
        result = False

        sql = "SELECT null FROM students WHERE StudentID = %s AND password = %s"
        args = (userID, password)
        self.cursor.execute(sql, args)
        if self.cursor.rowcount == 1:
            #print("OHYA")
            result = True

        return result

    def isFriend(self, user, friend) :
        self.re_connect()
        # global cursor
        result = False
        if(self.confirmAccount(user) and self.confirmAccount(friend)) :
            sql = "SELECT null FROM friendMap WHERE user = %s AND friend = %s"
            args = (user, friend)
            self.cursor.execute(sql, args)
            if self.cursor.rowcount > 0:
                result = True

        return result

    def hasRoom(self, user, code) :
        self.re_connect()
        result = False
        sql = "SELECT null FROM roommap WHERE member = %s AND code = %s"
        args = (user, code)
        self.cursor.execute(sql, args)
        if self.cursor.rowcount > 0 :
            result = True
        return result

    def addFriend(self, user, friend) :
        self.re_connect()
        # global cursor, conn
        result = False
        if(self.confirmAccount(user) and self.confirmAccount(friend) and not(self.isFriend(user,friend)) and user != friend) :
            try :
                sql = "INSERT INTO friendMap(user, friend) VALUES(%s, %s)"
                args = (user, friend)
                self.cursor.execute(sql, args)
                self.conn.commit()
                sql = "INSERT INTO friendMap(user, friend) VALUES(%s, %s)"
                args = (friend, user)
                self.cursor.execute(sql, args)
                self.conn.commit()
                L = list((user, friend))
                if self.createChatRoom(L) :
                    result = True
            except :
                self.conn.rollback()

        return result

    def deleteFriend(self, user, friend, code) :
        self.re_connect()
        # global cursor, conn
        result = False
        try :
            sql = "DELETE FROM friendMap WHERE user = %s AND friend = %s"
            args = (user, friend)
            self.cursor.execute(sql, args)
            self.conn.commit()
            sql = "DELETE FROM friendMap WHERE user = %s AND friend = %s"
            args = (friend, user)
            self.cursor.execute(sql, args)
            self.conn.commit()
            sql = "DELETE FROM record WHERE code = %s"
            args = (code)
            self.cursor.execute(sql, args)
            self.conn.commit()
            sql = "DELETE FROM roomMap WHERE code = %s"
            args = (code)
            self.cursor.execute(sql, args)
            self.conn.commit()
            result = True
        except :
            self.conn.rollback()

        return result

    def getFriendList(self, user) :
        self.re_connect()
        # global cursor, conn
        friendList = list(())
        try :
            sql = "SELECT friend FROM friendMap WHERE user = %s"
            args = (user)
            self.cursor.execute(sql, args)
            for i in range(0,self.cursor.rowcount) :
                row = self.cursor.fetchone()
                friendList.append(row[0])
        except :
            self.conn.rollback()

        return friendList

    def withdrawFromGroup(self, user, code) :
        self.re_connect()
        # global cursor, conn
        result = False
        try :
            sql = "DELETE FROM roomMap WHERE code = %s AND member = %s"
            args = (code, user)
            self.cursor.execute(sql, args)
            self.conn.commit()
            result = True
        except :
            self.conn.rollback()

        return result

    def createChatRoom(self, memberList, Type = "F", groupName = None) :
        self.re_connect()
        # global cursor, conn
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
                self.cursor.execute("INSERT INTO RoomMap(code,GroupName,member,type) VALUES(%s,%s,%s,%s)", (code,groupName,memberList[i],Type))
                self.conn.commit()
            result = True
        except :
            self.conn.rollback()

        return result

    def getInitInfo(self, user) :
        self.re_connect()
        #global cursor
        initInfo = list(())

        friendList = self.getFriendList(user)
        for i in range(0,len(friendList)) :
            friend = friendList[i]
            if self.cmp(user, friend) < 0 :
                string = "%s%s" % (user, friend)
            else :
                string = "%s%s" % (friend, user)
            code = self.MD5(string)
            name = self.getName(friend)
            memberID = self.getRoomMember(code)
            intro = self.getUserIntro(friend)
            initInfo.append(ChatRoom.ChatRoom(code, name, memberID, "F", intro))
        sql = "SELECT code, GroupName, Type FROM RoomMap WHERE GroupName IS NOT NULL AND member = %s"
        args = (user)
        self.cursor.execute(sql, args)
        for i in range(0,self.cursor.rowcount) :
            row = self.cursor.fetchone()
            initInfo.append(ChatRoom.ChatRoom(row[0], row[1], "", row[2], ""))
        for i in range(0,len(initInfo)) :
            room = initInfo[i]
            room.memberID = self.getRoomMember(room.code)
        return initInfo

    def getRoomName(self,code) :
        self.re_connect()
        sql = "SELECT GroupName FROM roommap WHERE code = %s"
        args = (code)
        self.cursor.execute(sql, args)
        row = self.cursor.fetchone()
        return row[0]

    def getRoomMember(self, code):
        self.re_connect()
        # global cursor
        member = ""
        sql = "SELECT member FROM roommap WHERE code = %s"
        args = (code)
        self.cursor.execute(sql, args)
        for i in range(0,self.cursor.rowcount) :
            row = self.cursor.fetchone()
            if i == 0:
                member += "%s" % (row[0])
            else :
                member += "-%s" % (row[0])
        return member

    def getRoomType(self, code):
        roomType = ""
        sql = "SELECT Type FROM roommap WHERE code = %s LIMIT 1"
        args = (code)
        self.cursor.execute(sql, args)
        if self.cursor.rowcount > 0:
            roomType = self.cursor.fetchone()[0]
        return roomType


    def getReceiverList(self, code) :
        self.re_connect()
        # global cursor
        receiverList = list(())

        sql = "SELECT member FROM RoomMap WHERE code = %s"
        args = (code)
        self.cursor.execute(sql, args)
        for i in range (0,self.cursor.rowcount) :
            row = self.cursor.fetchone()
            receiverList.append(row[0])

        return receiverList

    def inviteNewFriend(self, code, roomName, memberID) :
        self.re_connect()
        try :
            sql = "INSERT INTO roommap(code,GroupName,member,Type) VALUES(%s,%s,%s,'G')"
            args = (code, roomName, memberID)
            self.cursor.execute(sql, args)
            self.conn.commit()
        except:
            self.conn.rollback()

    def storeRecord(self, code, sender, MSG, type_t = 'text') :
        self.re_connect()
        # global cursor, conn
        try :
            sql = "INSERT INTO Record(code,sender,MSG,type) VALUES(%s,%s,%s,%s)"
            args = (code, sender, MSG, type_t)
            self.cursor.execute(sql, args)
            self.conn.commit()
        except :
            self.conn.rollback()
        sql = "SELECT time FROM Record WHERE code = %s AND sender = %s AND MSG = %s ORDER BY time DESC LIMIT 1"
        args = (code, sender, MSG)
        self.cursor.execute(sql, args)
        row = self.cursor.fetchone()
        return row[0]


    def arrangeRecord(self, code) :
        self.re_connect()
        # global cursor, conn
        try :
            sql = "SELECT null FROM Record WHERE code = %s"
            args = (code)
            self.cursor.execute(sql, args)
            keep = 50
            if self.cursor.rowcount > keep :
                sql = "DELETE FROM Record WHERE code = %s ORDER BY time ASC LIMIT %d"
                args = (code, self.cursor.rowcount - keep)
                self.cursor.execute(sql, args)
                self.conn.commit()

        except :
            self.conn.rollback()


    def getRecord(self, code, record_cnt, last_pk) :
        self.re_connect()

        # in pymysql, convert all args to str type
        # so %d should replace with %s
        # or will cause TypeError: %d format: a number is required, not str
        sql = "SELECT null FROM Record WHERE code = %s AND PK <= %s"
        args = (code, last_pk)
        self.cursor.execute(sql, args)
        count = self.cursor.rowcount
        cap = 12
        offset = cap * (record_cnt - 1)
        if offset + cap > count :
            if offset + 1 > count :
                return None
            else :
                cap = count - offset

        '''
        cap = 12
        offset = count - cap * record_cnt
        if offset < 0 :
            if offset * -1 > cap :
                return None
            else :
                cap = cap + offset
                offset = 0
        '''

        record = list(())
        sql = "SELECT sender, MSG, time, type FROM Record WHERE code = %s AND PK <= %s ORDER BY PK DESC LIMIT %s,%s"
        args = (code, last_pk, offset, cap)
        self.cursor.execute(sql, args)

        for i in range(0,self.cursor.rowcount) :
            row = self.cursor.fetchone()
            record.append(Record.Record(row[0], row[1], row[2], row[3]))

        return record

    def getLastMSG(self, code) :
        self.re_connect()
        # global cursor
        sql = "SELECT MSG, type FROM record WHERE code = %s ORDER BY time DESC LIMIT 1"
        args = (code)
        self.cursor.execute(sql, args)
        if self.cursor.rowcount > 0 :
            row = self.cursor.fetchone()
            if row[1] == 'text' :
                return row[0]
            else :
                return 'a new image'
        else :
            return 'No History'
    def getLastMSGTime(self, code) :
        self.re_connect()
        # global cursor
        sql = "SELECT time FROM record WHERE code = %s ORDER BY time DESC LIMIT 1"
        args = (code)
        self.cursor.execute(sql, args)
        if self.cursor.rowcount > 0:
            row = self.cursor.fetchone()
            date = datetime.strftime(row[0],'%Y-%m-%d %H:%M:%S')
            return date
        else :
            return "XXXX-XX-XX XX:XX"

    def getLastMsgPk(self, code) :
        self.re_connect()
        sql = "SELECT PK FROM record WHERE code = %s ORDER BY PK DESC LIMIT 1"
        args = (code)
        self.cursor.execute(sql, args)
        if self.cursor.rowcount > 0 :
            last_pk = self.cursor.fetchone()[0]
            return last_pk
        else :
            return 0


    def getLast(self, user, Type) :
        self.re_connect()
        # global cursor
        if Type == "F" :
            sql = "SELECT code FROM RoomMap WHERE member = %s AND Type = %s ORDER BY time DESC LIMIT 1"
            args = (user, Type)
            self.cursor.execute(sql, args)
            row = self.cursor.fetchone()
            return row[0]
        elif Type == "G" :
            sql = "SELECT code, GroupName FROM RoomMap WHERE member = %s AND Type = %s ORDER BY time DESC LIMIT 1"
            args = (user, Type)
            self.cursor.execute(sql, args)
            row = self.cursor.fetchone()
            return "%s/%s" % (row[0],row[1])

    def getImage(self, user) :
        self.re_connect()
        # global cursor
        sql = "SELECT Photo FROM students WHERE StudentID = %s"
        args = (user)
        self.cursor.execute(sql, args)
        row = self.cursor.fetchone()
        return row[0]

    def getUserImagePath(self, user) :
        self.re_connect()
        sql = "SELECT image_path FROM students WHERE StudentID = %s"
        args = (user)
        self.cursor.execute(sql, args)
        row = self.cursor.fetchone()
        return row[0]

    def getName(self, user) :
        # global cursor
        sql = "SELECT Name FROM students WHERE StudentID = %s"
        args = (user)
        self.cursor.execute(sql, args)
        name = self.cursor.fetchone()
        return name[0]

    def MD5(self, string) :
        encoder = hashlib.md5()
        encoder.update(string.encode('utf-8'))
        code = encoder.hexdigest().upper()
        return code

    def codeExist(self, code) :
        self.re_connect()
        # global cursor
        result = False
        sql = "SELECT DISTINCT null FROM RoomMap WHERE code = %s"
        args = (code)
        self.cursor.execute(sql, args)
        if self.cursor.rowcount > 0 :
            result = True

        return result

    def cmp(self, a, b) :
        return (a > b) - (a < b)

    def submitFCMToken(self, user, token) :
        try :
            sql = "INSERT INTO FCMToken(user, token) VALUES(%s, %s) ON DUPLICATE KEY UPDATE token = %s"
            args = (user, token, token)
            self.cursor.execute(sql, args)
            self.conn.commit()
        except :
            self.conn.rollback()

        return

    def findFCMToken(self, user) :
        result = "empty"
        sql = "SELECT token FROM FCMToken WHERE user = %s"
        args = (user)
        self.cursor.execute(sql, args)
        if self.cursor.rowcount > 0 :
            result = self.cursor.fetchone()

        return result[0]

    def createClass(self, className, keeper) :
        if not self.confirmAccount(keeper) :
            return False
        code = self.MD5(("class:%s, keeper:%s" % (className, keeper)))
        if self.codeExist(code) :
            return False
        else :
            try :
                sql = "INSERT INTO roommap(code, GroupName, member, Type) VALUES(%s, %s, %s, 'C')"
                args = (code, className, keeper)
                self.cursor.execute(sql, args)
                self.conn.commit()
                sql = "INSERT INTO classkeeper(code, className, keeper) VALUES(%s, %s, %s)"
                args = (code, className, keeper)
                self.cursor.execute(sql, args)
                self.conn.commit()
            except :
                self.conn.rollback()
            return True

    def addToClass(self, className, keeper, student) :
        if not self.confirmAccount(student) :
            return False
        sql = "SELECT code FROM classkeeper WHERE className = %s AND keeper = %s"
        args = (className, keeper)
        self.cursor.execute(sql, args)
        if self.cursor.rowcount > 0 :
            code = self.cursor.fetchone()[0]
            sql = "SELECT null FROM roommap WHERE member = %s"
            args = (student)
            self.cursor.execute(sql, args)
            if self.cursor.rowcount > 0 :
                return False
            try :
                sql = "INSERT INTO roommap(code, GroupName, member, Type) VALUES(%s, %s, %s, 'C')"
                args = (code, className, student)
                self.cursor.execute(sql, args)
                self.conn.commit()
            except :
                self.conn.rollback()
            return True
        else :
            return False

    def getClassKeeper(self, code) :
        sql = "SELECT keeper FROM classkeeper WHERE code = %s"
        args = (code)
        self.cursor.execute(sql, args)
        if self.cursor.rowcount > 0 :
            keeper = self.cursor.fetchone()[0]
            return keeper
        else :
            return ""

    def storePoster(self, code, sender, theme, MSG, type_t) :
        self.re_connect()
        try :
            sql = "INSERT INTO poster(code, sender, theme, MSG, type) VALUES(%s,%s,%s,%s,%s)"
            args = (code, sender, theme, MSG, type_t)
            self.cursor.execute(sql, args)
            self.conn.commit()
        except :
            self.conn.rollback()

        sql = "SELECT time FROM poster WHERE code = %s AND sender = %s AND theme = %s AND MSG = %s"
        args = (code, sender, theme, MSG)
        self.cursor.execute(sql, args)
        row = self.cursor.fetchone()
        return row[0]


    def fetchPost(self, code) :
        self.re_connect()
        record = list(())
        sql = "SELECT sender, Theme, MSG, time FROM poster WHERE code = %s AND type = %s"
        args =(code, "post")
        self.cursor.execute(sql, args)

        for i in range(0,self.cursor.rowcount) :
            row = self.cursor.fetchone()
            record.append(Post.Post(row[0],row[1],row[2],row[3],"post"))

        return record

    def fetchPostReply(self, code, theme) :
        self.re_connect()
        record = list()
        sql = "SELECT sender, MSG, time FROM poster WHERE code = %s AND theme = %s AND type = %s"
        args = (code, theme, "reply")
        self.cursor.execute(sql, args)

        for i in range(0,self.cursor.rowcount) :
            row = self.cursor.fetchone()
            record.append(Post.Post(row[0],theme,row[1],row[2],"reply"))

        return record

    def deletePost(self, code, theme) :
        self.re_connect()
        try :
            sql = "DELETE FROM poster WHERE code = %s AND Theme = %s"
            args = (code, theme)
            self.cursor.execute(sql, args)
            self.conn.commit()
        except :
            self.conn.rollback()

    def deleteReply(self, sender, code, theme, content) :
        self.re_connect()
        try :
            sql = "DELETE FROM poster WHERE sender = %s AND code = %s AND Theme = %s AND MSG = %s"
            args = (sender, code, theme, content)
            self.cursor.execute(sql, args)
            self.conn.commit()
        except :
            self.conn.rollback()


    def deleteMessage(self, sender, code, time) :
        self.re_connect()
        try :
            sql = "DELETE FROM record WHERE sender = %s AND code = %s AND time = %s"
            args = (sender, code, time)
            self.cursor.execute(sql, args)
            self.conn.commit()
        except :
            self.conn.rollback()

    def changeUserName(self, user, newName) :
        self.re_connect()
        try :
            sql = "UPDATE students SET Name = %s WHERE StudentID = %s"
            args = (newName, user)
            self.cursor.execute(sql, args)
            self.conn.commit()
        except :
            self.conn.rollback()
            return False
        else :
            return True

    def changeUserIntro(self, user, newIntro) :
        self.re_connect()
        try :
            sql = "UPDATE students SET intro = %s WHERE StudentID = %s"
            args = (newIntro, user)
            self.cursor.execute(sql, args)
            self.conn.commit()
        except :
            self.conn.rollback()
            return False
        else :
            return True

    def changeUserPassword(self, user, newPassword) :
        self.re_connect()
        try :
            sql = "UPDATE students SET password = %s WHERE StudentID = %s"
            args = (newPassword, user)
            self.cursor.execute(sql, args)
            self.conn.commit()
        except :
            self.conn.rollback()
            return False
        else :
            return True

    def getUserIntro(self, user) :
        self.re_connect()
        sql = "SELECT intro FROM students WHERE StudentID = %s"
        args = (user)
        self.cursor.execute(sql, args)
        if self.cursor.rowcount > 0 :
            intro = self.cursor.fetchone()[0]
            return intro

    def getPhoneNum(self, user) :
        self.re_connect()
        sql = "SELECT PhoneNum FROM students WHERE StudentID = %s"
        args = (user)
        self.cursor.execute(sql, args)
        if self.cursor.rowcount > 0 :
            phoneNum = self.cursor.fetchone()[0]
            return phoneNum

    def addAnnoc(self, announcer, code, text, due, a_type, vote_type, vote_item) :
        self.re_connect()
        result = False
        if a_type == '2' :
            if not vote_type :
                return False
            elif vote_type == 'MC' :
                if not vote_item :
                    return False
        try :
            sql = "INSERT INTO annoc(announcer, code, text, due) VALUES(%s, %s, %s, %s)"
            args = (announcer, code, text, due)
            self.cursor.execute(sql, args)
            self.conn.commit()
            last_id = self.cursor.lastrowid
            if a_type == '2' :
                if vote_type == 'AD' :
                    vote_item = "AD"
                sql = "INSERT INTO vote_item(annoc_pk, items) VALUES(%s, %s)"
                args = (last_id, vote_item)
                self.cursor.execute(sql, args)
                self.conn.commit()
            result = self.cursor.lastrowid
        except :
            self.conn.rollback()

        return result

    def getAnnoc(self, user) :
        annoc_list = list(())
        sql = "SELECT code FROM roommap WHERE member = %s AND Type = 'C'"
        args = (user)
        self.cursor.execute(sql, args)
        code_list = self.cursor.fetchall()
        if len(code_list) > 0:
            for code in code_list[0] :
                sql = "SELECT PK, text FROM annoc WHERE code = %s"
                args = (code)
                self.cursor.execute(sql, args)
                for i in range(0, self.cursor.rowcount) :
                    row = self.cursor.fetchone()
                    pk_text = "\n".join([str(row[0]), row[1]])
                    annoc_list.append(pk_text)
                for i in range(0, len(annoc_list)) :
                    annoc_pk = annoc_list[i].split('\n')[0]
                    sql = "SELECT items FROM vote_item WHERE annoc_pk = %s AND items != 'AD'"
                    args = (annoc_pk)
                    self.cursor.execute(sql, args)
                    if self.cursor.rowcount > 0 :
                        annoc_list[i] = annoc_list[i] + ":::" + self.cursor.fetchone()[0]
        return annoc_list

    def getImgMsgWithTime(self) :
        sql = "SELECT PK, MSG, time FROM record WHERE type = 'img' AND cleared = 'N'"
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def setClearedInRecord(self, pk) :
        try :
            sql = "UPDATE record SET cleared = 'Y' WHERE PK = %s"
            args = (pk)
            self.cursor.execute(sql, args)
            self.conn.commit()
        except :
            self.conn.rollback()

    def check_annoc_due(self) :
        try :
            time = datetime.now()
            time_str = datetime.strftime(time, '%Y-%m-%d %H:%M:%S')
            sql = "DELETE FROM annoc WHERE due < %s"
            args = (time_str)
            self.cursor.execute(sql, args)
            self.conn.commit()
        except :
            self.conn.rollback()

    def add_vote_result(self, pk, user, selected) :
        sql = "SELECT null FROM vote_result WHERE annoc_pk = %s AND id = %s"
        args = (pk, user)
        self.cursor.execute(sql, args)
        if self.cursor.rowcount == 0 :
            try :
                sql = "INSERT INTO vote_result(annoc_pk, id, result) VALUES(%s, %s, %s)"
                args = (pk, user, selected)
                self.cursor.execute(sql, args)
                self.conn.commit()
            except :
                self.conn.rollback()
                return False
            else :
                return True
        else :
            return False




############################################################
if __name__ == "__main__" :
    conn = DBHandler.connect()
    cursor = conn.cursor()
    db = DBHandler(conn,cursor)
    if db.addFriend("F74056255", "M40021010") :
        print("add friend succeed")
    else :
        print("error adding friend")
