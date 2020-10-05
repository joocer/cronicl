"""
Common Pumps

A set of prewritten data pumps for reuse.

Pumps generate data to be put through the pipeline.
"""
import time
from ._stage import Stage
from ._messages import create_new_message
try:
    import ujson as json
except ImportError:
    import json

#####################################################################

class FilePump(Stage):
    """
    Reads records from a file
    """
    def init(self, **kwargs):
        try:
            self.sample_rate = float(kwargs.get('sample_rate', 0))
        except:
            self.sample_rate = 0.01


    def execute(self, record):
        """
        Note that this will be instantiated once and read the
        file to the end.
        """
        with open(record, 'r', encoding='utf-8') as file:
            row = file.readline()
            while row:
                yield create_new_message(row, sample_rate=self.sample_rate)
                row = file.readline()

#####################################################################

class JSONLFilePump(Stage):
    """
    Reads records from a file
    """
    def execute(self, record):
        """
        Note that this will be instantiated once and read the
        file to the end.
        """

        limit = -1

        with open(record.payload, 'r', encoding='utf-8') as file:
            row = file.readline()
            while row:

                yield create_new_message(json.loads(row))
                row = file.readline()

                limit -= 1
                if limit == 0:
                    row = None

#####################################################################

class TimerPump(Stage):
    """
    Initiates a message at an interval.

    This is not an accurate timer!

    DO NOT use for anything where the time is sensitive, it's 
    intended only to periodically intiate a flow.
    """
    def __init__(self, interval):
        self.interval = float(interval)
        Stage.__init__(self)

    def execute(self, interval):
        # send a message at the start
        last_time = time.time()
        yield create_new_message(last_time)

        if self.interval == None:
            self.interval - float(interval)

        while True:
            time.sleep(0.1)
            this_time = time.time()
            if this_time - last_time >= self.interval:
                yield create_new_message(this_time)
                last_time = this_time
