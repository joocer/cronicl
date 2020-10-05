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

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format="%(created)f %(message)s")

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
            yield message
        except:
            print(message)
            yield None 

class MostFollowersStage(cronicl.stages.Stage):

    def init(self, **kwargs):
        self.user = ''
        self.followers = 0

    def execute(self, message):
        payload = message.payload
        if payload.get('followers') > self.followers:
            self.followers = payload.get('followers')
            self.user = payload.get('user')
            yield message



dag = nx.DiGraph()

#dag.add_node('JSON File Pump', function=cronicl.stages.JSONLFilePump())
dag.add_node('Data Validation', function=cronicl.stages.ValidatorStage({ "followers": "numeric", "username" : "string" }))
dag.add_node('Extract Followers', function=ExtractFollowersStage())
dag.add_node('Most Followers (verified)', function=MostFollowersStage())
dag.add_node('Most Followers (unverified)', function=MostFollowersStage())
dag.add_node('Screen Sink', function=cronicl.stages.ScreenSink())

#dag.add_edge('JSON File Pump', 'Data Validation')
dag.add_edge('Data Validation', 'Extract Followers')
dag.add_edge('Extract Followers', 'Most Followers (verified)', filter=lambda x: x.payload.get('verified') == True )
dag.add_edge('Extract Followers', 'Most Followers (unverified)', filter=lambda x: x.payload.get('verified') == False )
dag.add_edge('Most Followers (verified)', 'Screen Sink')
dag.add_edge('Most Followers (unverified)', 'Screen Sink')

flow = cronicl.Pipeline(dag, sample_rate=0.001)
flow.init()
flow.draw()
flow.execute(cronicl.datasets.io.read_jsonl('small.jsonl'))
flow.close()

for node in dag.nodes():
    try:
        print(dag.nodes()[node].get('function').read_sensor())
    except:
        print('failed to read sensors on:', node)