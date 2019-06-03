from pyfcm import FCMNotification

mykey = "AAAAGIavIEU:APA91bEgwRR5TUYnkw1InBYyA-b0YLdABXOaBid_zEkDBMeYWajDCvi5HmUncaCTj98grwbnilho6ZlBdTon8CvPQ3-tpqvP9B2k9x0fSAzFsu5IHlPhUY1bv4ai4ATYj2RpBQCbrjQ_"

def push_notify_to_one(registration_id, mt, mb, code, userID, roomType) :
    global mykey
    push_service = FCMNotification(api_key = mykey)

    data_message = {
        "mod" : "0",
        "msgTitle" : mt,
        "msgText" : mb,
        "code" : code,
        "userID" : userID,
        "roomType" : roomType
    }

    result = push_service.single_device_data_message(registration_id=registration_id, data_message=data_message)

def push_notify_annoc(token, mt, text) :
    global mykey
    push_service = FCMNotification(api_key = mykey)

    data_message = {
        "mod" : "1",
        "title" : mt,
        "text" : text
    }

    result = push_service.single_device_data_message(registration_id=token, data_message=data_message)
