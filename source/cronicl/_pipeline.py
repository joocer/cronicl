import time, logging, warnings
import networkx as nx
from .stages import PassThruStage, create_new_message
from .stages._trace import Trace
from ._queue import get_queue, queues_empty
import uuid

import threading

class Pipeline(object):

    def __init__(self, graph, sample_rate=0.001):
        self.threads = []
        self.paths = { }
        self.graph = graph
        self.all_stages = self.graph.nodes()
        self.initialized = False

        # tracing can be resource heavy, so we trace a sample
        # default sampling rateis 0.1% (one per thousand)
        self.sample_rate = sample_rate 

        # get the entry nodes - the ones with 0 incoming nodes
        self.entry_nodes = [ node for node in self.all_stages if len(graph.in_edges(node)) == 0 ]

        # VALIDATE THE GRAPH
        # The pipeline can't be cyclic
        has_loop = True
        try:
            nx.find_cycle(graph, orientation="ignore")
        except nx.NetworkXNoCycle:
            has_loop = False
        if has_loop:
            raise Exception("Pipeline must not be cyclic, if unsure do not have more than on incoming connection on any stages.")

        # Every stage node must have a function attribute
        if not all([graph.nodes()[node].get('function') for node in self.all_stages]):
            raise Exception("All stages in the Pipeline must have a 'function' attribute")

        # Every object on the function attribute must have an execute method
        if not all([hasattr(graph.nodes()[node]['function'], 'execute') for node in self.all_stages]):
            raise Exception("The object on all 'function' attributes in a Pipeline must have an 'execute' method")

        logging.debug('loaded a pipeline with {} stages, {} entry point(s)'.format(len(self.all_stages), len(self.entry_nodes)))


    def reply_handler(self):
        """
        Accept messages on the reply queue, replies attest the stage
        they have come from, we work out the next stages and put
        the message onto their queue.

        queue.get() is blocking so this should be run in a separate
        thread.

        This is called for every message in the system, so for 
        performance it caches key information. Although not a pure
        function, it is deterministic and idempotent, which should
        make it thread-safe.
        """
        queue = get_queue('reply')
        response = queue.get()
        while response:
            respondent, message = response
            # If it's the first time we've seen this respondent, 
            # cache its path.
            # Invalidating this cache will allow us to update the
            # DAG in a running pipeline.
            if not self.paths.get(respondent):
                self.paths[respondent] = []
                outgoing_edges = self.graph.out_edges(respondent, default=[])
                for this_stage, next_stage in outgoing_edges:
                    data_filter = self.graph.get_edge_data(respondent, next_stage).get('filter', lambda x: True)
                    self.paths[respondent].append((next_stage, data_filter))
            
            for next_stage, data_filter in self.paths.get(respondent):
                if message:
                    if data_filter(message):
                        get_queue(next_stage).put(message, False)

            response = queue.get()

        # None is used to terminate the handler
        logging.debug("REPLY handler got TERM signal")


    def init(self, **kwargs):

        # call all the stage inits, pass the kwargs
        for stage in self.all_stages:
            stage_function = self.graph.nodes()[stage].get('function', PassThruStage())
            if hasattr(stage_function, 'init'):
                stage_function.init(**kwargs)
            # stages need to be told their name
            stage_function.stage_name = stage 
        self.initialized = True

        for stage in self.all_stages:
            # Stages can define a number of threads to create.
            #
            # This is intended to be used by stages with IO, if the
            # stage is just working in memory, multi-threading is
            # not advised as locks are likely to cause slow 
            # processing. It is important to remember, Python does
            # not concurrently run threads, one thread runs whilst
            # the other wait.
            thread_count = self.graph.nodes()[stage].get('threads', 1)
            # clamp the number of threads between 1 and 5
            thread_count = 1 if thread_count < 1 else 5 if thread_count > 5 else thread_count
            
            stage_function = self.graph.nodes()[stage].get('function', PassThruStage())
            for i in range(thread_count):
                thread=threading.Thread(target=stage_function.run)
                thread.daemon = True
                thread.start()
                self.threads.append(thread)

        # Create multiple threads to handle replies
        # The reply_handler has no locks and testing has shown that
        # multiple handlers increases overall pipeline throughput
        for i in range(2):
            reply_handler_thread = threading.Thread(target=self.reply_handler)
            reply_handler_thread.daemon = True
            reply_handler_thread.start()
            self.threads.append(reply_handler_thread)


    def execute(self, value):
        """
        """
        if not self.initialized:
            raise Exception("Pipeline's init method must be called before execute")
        
        # if the value isn't iterable, put it in a list
        if not type(value).__name__ in ['generator', 'list']:
            value = [value]

        # Create the message envelopes and execute the pipeline from
        # the entry nodes 
        for v in value:
            message = create_new_message(v, sample_rate=self.sample_rate)
            for entry in self.entry_nodes:
                get_queue(entry).put(message, False)

        return


    def close(self):

        if self.initialized:
            # call all the closes
            for stage in self.all_stages:
                stage_function = self.graph.nodes()[stage].get('function', PassThruStage())
                if hasattr(stage_function, 'close'):
                    stage_function.close()


    def running(self):
        return not queues_empty()


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
        print('Pipeline Entry')
        for entry in self.entry_nodes:
            print(' └─ {}'.format(entry))
            t = self.tree(entry, '    ')
            print('\n'.join(t))
