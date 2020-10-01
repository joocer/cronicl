import networkx as nx
import logging, sys, time
import abc

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format="%(created)f %(message)s")

#####################################################################

# base class
class Stage(abc.ABC):

    class Sensor(object):
        """
        Decorator for Instrumentation.

        Count the number of records which have been returned by a 
        process. The record count is saved to the process.

        Records the first and last time the decorated method is 
        called.
        """
        def __init__(self):
            self.first_run = True
            self.parent = None

        def __call__(self, function):

            def inner_func(*args):
                try:
                    if self.first_run:
                        self.first_run = False
                        self.parent = args[0]
                        self.parent.first_seen = time.time()

                    self.parent.input_record_count += 1
                   
                    # this is the execution payload
                    return_values = function(*args)

                    for return_value in return_values:
                        self.parent.output_record_count += 1
                        yield return_value
                except:
                    logging.error('an exception occurred which needs a better error message')
                    return

            return inner_func

    def __init__(self):
        self.input_record_count = 0
        self.output_record_count = 0
        self.first_seen = 0
        self.execution_time = 0


    @abc.abstractmethod
    def execute(self, record):
        """
        The payload, this must be overridden
        """
        yield record


    def __call__(self, record):
        """
        Alias for execute
        """
        yield from self.execute(record)


    def close(self):
        pass


    def read_sensor(self):
        return { 
            'process': self.__class__.__name__,
            'input_records': self.input_record_count,
            'output_records': self.output_record_count,
            'execution_start': self.first_seen,
            'execution_time': self.execution_time / 1e9
        }

#####################################################################

class ScreenSink(Stage):

    @Stage.Sensor()
    def execute(self, record):
        print('>>>', record)
        return []


class WorkingHoursStage(Stage):

    @Stage.Sensor()
    def execute(self, record):
        hours = { True: 7, False: 0 }
        record['working_hours'] = hours[record['weekday']]
        yield record


class PassThruStage(Stage):

    @Stage.Sensor()
    def execute(self, record):
        yield record

#####################################################################

def always(x):
    return True


def empty(x):
    return []

#####################################################################

def execute(pipeline, source):
    # get all the nodes with no incoming edges
    pumps = [ node for node in dag.nodes() if len(dag.in_edges(node)) == 0 ]
    for pump in pumps:
        # run the state and force the expansion of the results to
        # run the pipeline, the sinks should persist the results
        # so we should be able to discard the final results.
        [always(x) for x in (_inner_execute(pipeline, pump, source) or [])]


def _inner_execute(pipeline, node, record):
    """
    Execute the stage, and then find out-going edges, execute any 
    filters defined on the edge and then execute the connected
    stage nodes.
    """
    stage = pipeline.nodes()[node]
    outgoing_edges = pipeline.out_edges(node, default=[])
    
    # the primary payload
    results = stage.get('stage', empty)(record)

    # we don't know if we have any results, 'or []' handles 'None'
    for result in (results or []):
        # for each out-going edge
        for outgoing_edge in outgoing_edges:
            next_stage = outgoing_edge[1]
            # get the filter associated with the edge
            data_filter = pipeline.get_edge_data(node, next_stage).get('filter', always)
            # if the data 'passes' the filter, execute the next stage
            if data_filter(result):
                yield from _inner_execute(pipeline,next_stage, result)
    return

#####################################################################

dag = nx.DiGraph()

"""
This Graph is this:

                  ┌── stage 2.1 ── sink 1
pump ── stage 1 ──┤                       
                  └── stage 2.2 ── sink 2
│┼
"""

dag.add_node('pump', stage=PassThruStage())
dag.add_node('stage 1', stage=WorkingHoursStage())
dag.add_node('stage 2.1', stage=PassThruStage())
dag.add_node('stage 2.2', stage=PassThruStage())
dag.add_node('sink 1', stage=ScreenSink())
dag.add_node('sink 2', stage=PassThruStage())

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

for node in dag.nodes():
    print(dag.nodes()[node].get('stage').read_sensor())


logging.debug('done')
