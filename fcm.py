from pyfcm import FCMNotification


def push_notify_to_one(registration_id, mt, mb, code) :
    push_service = FCMNotification(api_key = "AAAAGIavIEU:APA91bEgwRR5TUYnkw1InBYyA-b0YLdABXOaBid_zEkDBMeYWajDCvi5HmUncaCTj98grwbnilho6ZlBdTon8CvPQ3-tpqvP9B2k9x0fSAzFsu5IHlPhUY1bv4ai4ATYj2RpBQCbrjQ_")

    data_message = {
        "msgTitle" : mt,
        "msgText" : mb,
        "code" : code
    }

    result = push_service.single_device_data_message(registration_id=registration_id, data_message=data_message)


