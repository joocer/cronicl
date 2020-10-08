import uuid
import logging
import random # we're using to select random samples so prng is good enough
try:
    import ujson as json
except ImportError:
    import json
from ._trace import Trace


class Message(object):

    def __init__(self, payload=None, traced=False):
        self.id = str(uuid.uuid4())
        self.payload = payload
        self.attributes = {}
        self.traced = traced

    def __repr__(self):
        return self.payload

    def __str__(self):
        return str(self.payload)

    def trace(self, stage='not defined', spawned=''):
        if self.traced:
            record = ''
            try:
                record = json.dumps(self.payload)
            except:
                record = str(self.payload)
            Trace.emit(self.id, stage, spawned, record)

def create_new_message(payload, sample_rate=0):
    if sample_rate > 0:
        traced = random.randint(1, round(1/sample_rate)) == 1
    else:
        traced = False
    message = Message(payload=payload, traced=traced)
    message.trace(stage='creation')
    return message