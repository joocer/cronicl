import networkx as nx
import logging, sys
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format="%(created)f %(message)s")

# nodes are stages
# node can be a pump
# nodes can be sinks
# edges are filters


# base class
class Stage(object):
    def __init__(self):
        pass

    def execute(self, record):
        yield record

    def __call__(self, record):
        yield from self.execute(record)

# node - write data out
class Sink(Stage):
    def __init__(self):
        pass

# node - data generator
class Pump(Stage):
    def __init__(self):
        pass


class ScreenSink(Sink):

    def execute(self, record):
        print('>>>', record)
        yield

class WorkingHoursStage(Stage):

    def execute(self, record):
        hours = { True: 7, False: 0 }
        record['working_hours'] = hours[record['weekday']]
        yield record


def always(x):
    return True

def empty(x):
    return []


def execute(pipeline, source):
    pumps = [x for x,y in pipeline.nodes(data=True) if y.get('type') == "pump"]
    for pump in pumps:
        results = _inner_execute(pipeline, pump, source)
    return list(results)


def _inner_execute(pipeline, node, record):

    #logging.debug("_inner_execute")

    e = pipeline.nodes()[node]
    
    transitions = pipeline.out_edges(node, default=[])
    
    #logging.debug('record: {}'.format(record))
    r = e.get('func', empty)(record)
    for i in r:
        #logging.debug('i: {}'.format(i))
        for t in transitions:
            #logging.debug('t: {}'.format(t))
            data_filter = pipeline.get_edge_data(t[0], t[1]).get('filter', always)
            if data_filter(i):
                #logging.debug('filtered: {}'.format(i))
                yield from _inner_execute(pipeline, t[1], i)
    return

    

dag = nx.DiGraph()

"""
This Graph is this:

                  ┌── stage 2.1 ── sink 1
pump ── stage 1 ──┤                       
                  └── stage 2.2 ── sink 2
│┼
"""

dag.add_node('pump', type="pump", func=Pump())
dag.add_node('stage 1', type="stage", func=WorkingHoursStage())
dag.add_node('stage 2.1', type="state", func=Stage())
dag.add_node('stage 2.2', type="state", func=Stage())
dag.add_node('sink 1', type="sink", func=ScreenSink())
dag.add_node('sink 2', type="sink", func=Sink())

dag.add_edge('pump', 'stage 1')
dag.add_edge('stage 1', 'stage 2.1', filter=lambda x: x.get('weekday') == True )
dag.add_edge('stage 1', 'stage 2.2', filter=lambda x: x.get('weekday') == False )
dag.add_edge('stage 2.1', 'sink 1')
dag.add_edge('stage 2.2', 'sink 2')

dow = [ { "day": "Monday", "weekday": True },
        { "day": "Tuesday", "weekday": True },
        { "day": "Wednesday", "weekday": True },
        { "day": "Thursday", "weekday": True },
        { "day": "Friday", "weekday": True },
        { "day": "Saturday", "weekday": False },
        { "day": "Sunday", "weekday": False } ]


for d in dow:
    execute(dag, d)

logging.debug('done')
