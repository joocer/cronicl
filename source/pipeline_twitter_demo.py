"""
Pipeline Demo - Twitter

A flow with custom stages, forking, filtering and merging.

JSON File Pump
 └─ Data Validation
     └─ Extract Followers
         ├─ Most Followers (verified)
         │   └─ Screen Sink
         └─ Most Followers (unverified)
             └─ Screen Sink <same instance as Screen Sink above>
"""

import cronicl
from cronicl.stages import create_new_message 
import logging, sys
import networkx as nx
import time

# this is the mechanism used for the message tracing.
#logging.basicConfig( level=logging.DEBUG, format="%(created)-15s %(message)s")
#logging.getLogger('').addHandler(logging.NullHandler)

class ExtractFollowersStage(cronicl.stages.Stage):

    def execute(self, message):
        payload = message.payload
        try:
            followers = int(payload.get('followers', -1))
            verified = payload.get('user_verified', 'False') == 'True'
            result = { 
                "followers": followers, 
                "user": payload.get('username', ''), 
                "verified": verified }
            message.payload = result
            return [message]
        except:
            print(message)
            return None 

class MostFollowersStage(cronicl.stages.Stage):

    def init(self, **kwargs):
        self.user = ''
        self.followers = 0
        cronicl.stages.Stage.init(self)

    def execute(self, message):
        payload = message.payload
        if payload.get('followers') > self.followers:
            self.followers = payload.get('followers')
            self.user = payload.get('user')
            return [message]



dag = nx.DiGraph()

dag.add_node('Data Validation', function=cronicl.stages.ValidatorStage({ "followers": "numeric", "username" : "string" }), threads=1)
dag.add_node('Extract Followers', function=ExtractFollowersStage(), threads=1)
dag.add_node('Most Followers (verified)', function=MostFollowersStage())
dag.add_node('Most Followers (unverified)', function=MostFollowersStage())
dag.add_node('Screen Sink', function=cronicl.stages.ScreenSink())

dag.add_edge('Data Validation', 'Extract Followers')
dag.add_edge('Extract Followers', 'Most Followers (verified)', filter=lambda x: x.payload.get('verified') == True )
dag.add_edge('Extract Followers', 'Most Followers (unverified)', filter=lambda x: x.payload.get('verified') == False )
dag.add_edge('Most Followers (verified)', 'Screen Sink')
#dag.add_edge('Most Followers (unverified)', 'Screen Sink')


def generator_chunker(gen, chunk_size):
    idx = 0
    chunk = [None] * chunk_size
    item = next(gen)
    while item:
        chunk[idx] = item
        idx += 1
        if idx == chunk_size:
            yield chunk
            idx = 0
        try:
            item = next(gen)
        except:
            item = None
    yield chunk[0:idx]


def main():
    cronicl.Trace().setHandler(cronicl.FileTracer('cronicl_trace.log'))

    flow = cronicl.Pipeline(dag, sample_rate=0)
    flow.init()
    flow.draw()

    with cronicl.timer.Timer():
        file_reader = cronicl.datasets.io.read_jsonl('small.jsonl', limit=2e6)  #5347923
        for chunk in generator_chunker(file_reader, 1000):
            #print(chr(27)+'[2j')
            #print('\033c')
            #print('\x1bc')
            flow.execute(chunk)
            #flow.running()

        while flow.running():
            time.sleep(1)

    flow.close()
    cronicl.Trace().close()

    for node in dag.nodes():
        try:
            print(dag.nodes()[node].get('function').read_sensor())
        except:
            print('failed to read sensors on:', node)



if __name__ == "__main__":
    main()