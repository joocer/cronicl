import time, logging
import networkx as nx
from .stages import PassThruStage


def always_pass(x):
    return True

def empty(x):
    return []


class Pipeline(object):

    def __init__(self, graph):
        self.graph = graph
        self.all_stages = self.graph.nodes()
        # validate the graph
        # - can't be cyclic
        # - can't have orphans
        # - each node has a function attribute
        # - the object on the function attribute as an execute method
        logging.debug('loaded a pipeline with {} stages'.format(len(self.all_stages)))

    def execute(self, record, **kwargs):
        """
        """
        # call all the inits, pass the kwargs
        for stage in self.all_stages:
            stage_function = self.graph.nodes()[stage].get('function', PassThruStage())
            if hasattr(stage_function, 'init'):
                stage_function.init(**kwargs)

        # get all the nodes with no incoming edges
        pumps = [ node for node in self.all_stages if len(self.graph.in_edges(node)) == 0 ]
        for pump in pumps:
            logging.debug('running pump \'{}\', for record: {}'.format(pump, record))
            # run the state and force the expansion of the results to
            # run the pipeline, the sinks should persist the results
            # so we should be able to discard the final results.
            [ always_pass(x) for x in (self._inner_execute(pump, record) or []) ]

        # call all the closes
        for stage in self.all_stages:
            stage_function = self.graph.nodes()[stage].get('function', PassThruStage())
            if hasattr(stage_function, 'close'):
                stage_function.close()


    def _inner_execute(self, stage_node, record):
        """
        Execute the stage, and then find out-going edges, execute any 
        filters defined on the edge and then execute the connected
        stage nodes.
        """
        stage = self.graph.nodes()[stage_node].get('function', PassThruStage())
        outgoing_edges = self.graph.out_edges(stage_node, default=[])
        
        # the primary payload
        results = stage(record)

        # we don't know if we have any results, 'or []' handles 'None'
        for result in (results or []):

            # None terminates the flow
            if result == None:
                continue

            # for each out-going edge
            for outgoing_edge in outgoing_edges:
                next_stage = outgoing_edge[1]
                # get the filter associated with the edge
                data_filter = self.graph.get_edge_data(stage_node, next_stage).get('filter', always_pass)
                # if the data 'passes' the filter, execute the next stage
                if data_filter(result):
                    yield from self._inner_execute(next_stage, result)
        return


        
