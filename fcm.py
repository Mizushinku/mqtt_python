from pyfcm import FCMNotification


def push_notify_to_one(registration_id, mt, mb) :
    push_service = FCMNotification(api_key = "AAAAGIavIEU:APA91bEgwRR5TUYnkw1InBYyA-b0YLdABXOaBid_zEkDBMeYWajDCvi5HmUncaCTj98grwbnilho6ZlBdTon8CvPQ3-tpqvP9B2k9x0fSAzFsu5IHlPhUY1bv4ai4ATYj2RpBQCbrjQ_")


    result = push_service.notify_single_device(registration_id=registration_id, message_title=mt, message_body=mb)


