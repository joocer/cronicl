"""
Pipeline Demo - Twitter

A flow with custom stages, forking, filtering and merging.
"""

import cronicl
import logging, sys
import networkx as nx

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format="%(created)f %(message)s")

class ExtractFollowersStage(cronicl.stages.Stage):

    def execute(self, record):
        try:
            followers = int(record.get('followers', -1))
            verified = record.get('user_verified', 'False') == 'True'
            result = { 
                "followers": followers, 
                "user": record.get('username', ''), 
                "verified": verified }
            yield result
        except:
            print(record)
            yield None 

class MostFollowersStage(cronicl.stages.Stage):

    def init(self, **kwargs):
        self.user = ''
        self.followers = 0

    def execute(self, record):
        if record.get('followers') > self.followers:
            self.followers = record.get('followers')
            self.user = record.get('user')
            yield record



dag = nx.DiGraph()

dag.add_node('JSON File Pump', function=cronicl.stages.JSONLFilePump())
dag.add_node('Extract Followers', function=ExtractFollowersStage())
dag.add_node('Most Followers (verified)', function=MostFollowersStage())
dag.add_node('Most Followers (unverified)', function=MostFollowersStage())
dag.add_node('Screen Sink', function=cronicl.stages.ScreenSink())

dag.add_edge('JSON File Pump', 'Extract Followers')
dag.add_edge('Extract Followers', 'Most Followers (verified)', filter=lambda x: x.get('verified') == True )
dag.add_edge('Extract Followers', 'Most Followers (unverified)', filter=lambda x: x.get('verified') == False )
dag.add_edge('Most Followers (verified)', 'Screen Sink')
dag.add_edge('Most Followers (unverified)', 'Screen Sink')

flow = cronicl.Pipeline(dag)
flow.draw()
flow.execute('small.jsonl')

for node in dag.nodes():
    try:
        print(dag.nodes()[node].get('function').read_sensor())
    except:
        print('failed to read sensors on:', node)