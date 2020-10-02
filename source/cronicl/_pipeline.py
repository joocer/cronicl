import time
import networkx as nx
from .stages import PassThruStage


def always(x):
    return True

def empty(x):
    return []


class Pipeline(object):

    def __init__(self):
        pass

    def execute(self, graph, source, **kwargs):
        """
        """
        # get references to all of the stages
        all_stages = [ node for node in graph.nodes() ]

        # call all the inits, pass the kwargs
        for stage in all_stages:
            stage_function = graph.nodes()[stage].get('function', PassThruStage())
            if hasattr(stage_function, 'init'):
                stage_function.init(**kwargs)

        # get all the nodes with no incoming edges
        pumps = [ node for node in all_stages if len(graph.in_edges(node)) == 0 ]
        for pump in pumps:
            # run the state and force the expansion of the results to
            # run the pipeline, the sinks should persist the results
            # so we should be able to discard the final results.
            [ always(x) for x in (self._inner_execute(graph, pump, source) or []) ]

        # call all the closes
        for stage in all_stages:
            stage_function = graph.nodes()[stage].get('function', PassThruStage())
            if hasattr(stage_function, 'close'):
                stage_function.close()


    def _inner_execute(self, graph, node, record):
        """
        Execute the stage, and then find out-going edges, execute any 
        filters defined on the edge and then execute the connected
        stage nodes.
        """
        stage = graph.nodes()[node].get('function', PassThruStage())
        outgoing_edges = graph.out_edges(node, default=[])
        
        # record amount of time to exdcute the step
        start_ns = time.time_ns()
        # the primary payload
        results = stage(record)
        stage.execution_time += time.time_ns() - start_ns

        # we don't know if we have any results, 'or []' handles 'None'
        for result in (results or []):
            # for each out-going edge
            for outgoing_edge in outgoing_edges:
                next_stage = outgoing_edge[1]
                # get the filter associated with the edge
                data_filter = graph.get_edge_data(node, next_stage).get('filter', always)
                # if the data 'passes' the filter, execute the next stage
                if data_filter(result):
                    yield from self._inner_execute(graph, next_stage, result)
        return
