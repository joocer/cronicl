import datetime

class Trace(object):

    @staticmethod
    def emit(msg_id, stage, spawned, record):
        print(datetime.datetime.now().isoformat(), msg_id, stage, spawned, record)