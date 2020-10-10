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
        self.file = open(filename, 'a', encoding='utf8')

    def emit(self, msg_id, topic, stage, version, child, record):
        self.file.write("{} id:{} topic:{:<16} stage:{:<24} version:{:<5} child:{} record:{}\n".format(
            datetime.datetime.now().isoformat(), 
            msg_id, 
            topic[:16], 
            stage[:24], 
            str(version)[:5],
            child, 
            record))

    def close(self):
        self.file.close()

