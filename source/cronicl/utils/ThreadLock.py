"""
Implements a threading lock as a context manager.

This allows a lock to be acquired and released like this:


with ThreadLock():    <-- acquires lock
    do something
    do something else
                      <-- releases lock

"""

import threading


class ThreadLock(object):

    def __init__(self):
        self.lock = threading.Lock()

    def __enter__(self):
        self.lock.acquire()

    def __exit__(self, type, value, traceback):
        self.lock.release()