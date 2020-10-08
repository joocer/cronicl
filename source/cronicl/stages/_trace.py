import datetime

# We are the Borg
# https://code.activestate.com/recipes/66531/

class Trace(object):
    """
    Handles writing trace logs out.
    Implemented as a Borg Singleton.
    """
    __shared_state = {}
    def __init__(self):
        self.__dict__ = self.__shared_state

    def open(self, filename):
        self.file = open(filename, 'a')

    def emit(self, msg_id, stage, child, record):
        # should test if it' a new day, if so rollover the file
        self.file.write("{} id:{} stage:{:<24} child:{} record:{}\n".format(datetime.datetime.now().isoformat(), msg_id, stage[:24], child, record))

    def close(self):
        self.file.close()

