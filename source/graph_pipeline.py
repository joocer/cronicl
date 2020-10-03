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

"""
This Graph is this:

                  ┌── stage 2.1 ── sink 1
pump ── stage 1 ──┤                       
                  └── stage 2.2 ── sink 2
│┼
"""

dag.add_node('pump', function=cronicl.stages.JSONLFilePump())
dag.add_node('screen', function=cronicl.stages.NullSink())
dag.add_node('extract followers', function=ExtractFollowersStage())
dag.add_node('stage 2.1', function=MostFollowersStage())
dag.add_node('stage 2.2', function=cronicl.stages.PassThruStage())
dag.add_node('sink 1', function=cronicl.stages.NullSink())
dag.add_node('sink 2', function=cronicl.stages.ScreenSink())

dag.add_edge('pump', 'extract followers')
dag.add_edge('pump', 'screen')

dag.add_edge('extract followers', 'stage 2.1', filter=lambda x: x.get('verified') == True )
dag.add_edge('extract followers', 'stage 2.2', filter=lambda x: x.get('user') in ['realDonaldTrump', 'ArianaGrande'] )
dag.add_edge('stage 2.1', 'sink 1')
dag.add_edge('stage 2.2', 'sink 2')

with cronicl.Timer() as t:
    p = cronicl.Pipeline(dag)
    p.execute('small.jsonl')

for node in dag.nodes():
    try:
        print(dag.nodes()[node].get('function').read_sensor())
    except:
        print('failed to read sensors on:', node)
