import threading


class ThreadLock(object):

    def __init__(self):
        self.lock = threading.Lock()

    def __enter__(self):
        self.lock.acquire()

    def __exit__(self, type, value, traceback):
        self.lock.release()