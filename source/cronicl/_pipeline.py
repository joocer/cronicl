import time, logging, warnings
import networkx as nx
from .stages import PassThruStage


def always_pass(x):
    return True


class Pipeline(object):

    def __init__(self, graph):
        self.graph = graph
        self.all_stages = self.graph.nodes()

        # VALIDATE THE GRAPH
        # must have exactly one pump
        pump = [ node for node in self.all_stages if len(graph.in_edges(node)) == 0 ]
        if len(pump) != 1:
            raise Exception("Pipelines must have exactly one 'pump' (nodes with no incoming edges)")
        self.pump = pump[0]

        # The pipeline can't be cyclic
        try:
            nx.find_cycle(graph, orientation="original")
        except:
            pass

        # Every stage node must have a function attribute
        if not all([graph.nodes()[node].get('function') for node in self.all_stages]):
            raise Exception("All stages in the Pipeline must have a 'function' attribute")

        # Every object on the function attribute must have an execute method
        if not all([hasattr(graph.nodes()[node]['function'], 'execute') for node in self.all_stages]):
            raise Exception("The object on all 'function' attributes in a Pipeline must have an 'execute' method")

        logging.debug('loaded a pipeline with {} stages'.format(len(self.all_stages)))

    def execute(self, record, **kwargs):
        """
        """
        # call all the inits, pass the kwargs
        for stage in self.all_stages:
            stage_function = self.graph.nodes()[stage].get('function', PassThruStage())
            if hasattr(stage_function, 'init'):
                stage_function.init(**kwargs)

        logging.debug('running pump \'{}\', for record: {}'.format(self.pump, record))
        # run the state and force the expansion of the results to
        # run the pipeline, the sinks should persist the results
        # so we should be able to discard the final results.
        [ always_pass(x) for x in [ x for x in (self._inner_execute(self.pump, record) or []) ] ]

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
        yield

    # adapted from https://stackoverflow.com/questions/9727673/list-directory-tree-structure-in-python
    def tree(self, node, prefix=''):

        space =  '    '
        branch = ' │  '
        tee =    ' ├─ '
        last =   ' └─ '

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
        t = self.tree(self.pump)
        print(self.pump)
        print('\n'.join(t))

        
