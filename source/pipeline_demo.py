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
                                      ┌── Most Followers ─ 
JSON File Pump ── Extract Followers ──┤                     Screen Sink   
                                      └── Most Followers ──
│┼
"""

dag.add_node('JSON File Pump', function=cronicl.stages.JSONLFilePump())
dag.add_node('Extract Followers', function=ExtractFollowersStage())
dag.add_node('Most Followers (verified)', function=MostFollowersStage())
dag.add_node('Most Followers (unverified)', function=MostFollowersStage())
dag.add_node('Screen Sink', function=cronicl.stages.ScreenSink())
#dag.add_node('File Sink', function=cronicl.stages.FileSink('target_2.jsonl'))

dag.add_edge('JSON File Pump', 'Extract Followers')
dag.add_edge('Extract Followers', 'Most Followers (verified)', filter=lambda x: x.get('verified') == True )
dag.add_edge('Extract Followers', 'Most Followers (unverified)', filter=lambda x: x.get('verified') == False )
dag.add_edge('Most Followers (verified)', 'Screen Sink')
dag.add_edge('Most Followers (unverified)', 'Screen Sink')
#dag.add_edge('Most Followers (unverified)', 'File Sink')


pipeline = cronicl.Pipeline(dag)

#with cronicl.Timer() as t:
#    pipeline.execute('small.jsonl')

for node in dag.nodes():
    try:
        print(dag.nodes()[node].get('function').read_sensor())
    except:
        print('failed to read sensors on:', node)

#pipeline.draw()
class drawer():



    def __init__(self, graph):
        self.graph = graph



    # adapted from https://stackoverflow.com/questions/9727673/list-directory-tree-structure-in-python
    def tree(self, node, prefix=''):

        space =  '    '
        branch = '│   '
        tee =    '├── '
        last =   '└── '

        contents = [ node[1] for node in self.graph.out_edges(node, default=[]) ]
        # contents each get pointers that are ├── with a final └── :
        pointers = [tee] * (len(contents) - 1) + [last]
        for pointer, child_node in zip(pointers, contents):
            yield prefix + pointer + child_node
            if len(self.graph.out_edges(node, default=[])) > 0: # extend the prefix and recurse:
                extension = branch if pointer == tee else space 
                # i.e. space because last, └── , above so no more |
                yield from self.tree(child_node, prefix=prefix+extension)

    def draw(self):
        pumps = [ node for node in self.graph.nodes() if len(self.graph.in_edges(node)) == 0 ]
        for pump in pumps:
            t = self.tree(pump)
            print(pump)
            print('\n'.join(t))

d = drawer(dag)
d.draw()