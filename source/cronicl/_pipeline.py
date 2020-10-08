import time, logging, warnings
import networkx as nx
from .stages import PassThruStage, create_new_message
import uuid


def always_pass(x):
    return True


class Pipeline(object):


    def __init__(self, graph, sample_rate=0.01):
        self.graph = graph
        self.all_stages = self.graph.nodes()
        self.initialized = False

        # tracing can be resource heavy, so we trace a sample
        # default sampling rateis 1%
        self.sample_rate = sample_rate 

        # get the nodes with 0 incoming nodes
        self.entry_nodes = [ node for node in self.all_stages if len(graph.in_edges(node)) == 0 ]

        # VALIDATE THE GRAPH
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

        logging.debug('loaded a pipeline with {} stages, {} entry point(s)'.format(len(self.all_stages), len(self.entry_nodes)))


    def set_up_tracing(self):
        import logging

        trace = logging.getLogger('cronicl_trace')

        trace_file = logging.handlers.TimedRotatingFileHandler('cronicl_trace', when='midnight', backupCount=7)
        trace_formatter = logging.Formatter("%(asctime)-15s %(message)s")
        trace_file.suffix = "%Y-%m-%d"
        trace_file.setLevel(logging.DEBUG)
        trace_file.setFormatter(trace_formatter)
        trace.addHandler(trace_file)



    def init(self, **kwargs):
        self.set_up_tracing()

        # call all the inits, pass the kwargs
        for stage in self.all_stages:
            stage_function = self.graph.nodes()[stage].get('function', PassThruStage())
            if hasattr(stage_function, 'init'):
                stage_function.init(**kwargs)
        self.initialized = True


    def execute(self, value):
        """
        """
        if not self.initialized:
            raise Exception("Pipeline's init method must be called before execute")

        #print(value, type(value))
        
        if type(value).__name__ in ['generator', 'list']:
            # we have something which creates multiple messages 
            # (like a file reader)
            #logging.debug('generator')
            # create the message envelopes and execute the pipeline 
            # from the entry nodes 
            for v in value:
                message = create_new_message(v, sample_rate=self.sample_rate)
                for entry in self.entry_nodes:
                    [ x for x in (self._inner_execute(entry, message)) ]

        else:
            # assume we have a single value to pump through the pipeline
            #logging.debug('other')
            # create the message envelopes and execute the pipeline 
            # from the entry nodes 
            message = create_new_message(value, sample_rate=self.sample_rate)
            for entry in self.entry_nodes:
                    [ x for x in (self._inner_execute(entry, message)) ]

        return


    def close(self):
        if self.initialized:
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
        #logging.debug('_inner_execute({}, {})'.format(stage_node, 5))

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
        print('instantiator')
        for entry in self.entry_nodes:
            t = self.tree(entry)
            print('\n'.join(t))

        
