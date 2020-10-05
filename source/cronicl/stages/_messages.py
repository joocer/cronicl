import uuid
import random # we're sampling so prng should be okay

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

def create_new_message(payload, sample_rate=0):
    if sample_rate > 0:
        traced = random.randint(1, round(1/sample_rate)) == 1
    else:
        traced = False
    return Message(payload=payload, traced=traced)