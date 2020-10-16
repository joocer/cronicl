import time, logging, warnings
import networkx as nx
from .stages import PassThruStage, create_new_message
from .stages._trace import Trace
from ._queue import get_queue, queues_empty
import uuid

import threading

class Pipeline(object):


    def __init__(self, graph, sample_rate=0.001, trace_sink='cronicl_trace'):
        self.threads = []
        self.paths = { }
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


    def reply_handler(self):

        queue = get_queue('reply')
        response = queue.get()
        while response:
            respondent, message = response

            logging.debug(f"I got a reply {message} from {respondent}")

            # if it's the first time we've seen this node, cache
            # it's path, this also should allow us to force a refresh
            if not self.paths.get(respondent):
                self.paths[respondent] = []
                outgoing_edges = self.graph.out_edges(respondent, default=[])
                for outgoing_edge in outgoing_edges:
                    next_stage = outgoing_edge[1]
                    data_filter = self.graph.get_edge_data(respondent, next_stage).get('filter', lambda x: True)
                    self.paths[respondent].append((next_stage, data_filter))
            
            for next_stage, data_filter in self.paths.get(respondent):
                if message:
                    if data_filter(message):
                        logging.debug(f'Ima gonna send to {next_stage}')
                        get_queue(next_stage).put(message)

            response = queue.get()
        logging.debug("REPLY handler got TERM signal")


    def init(self, **kwargs):

        # call all the inits, pass the kwargs
        for stage in self.all_stages:
            stage_function = self.graph.nodes()[stage].get('function', PassThruStage())
            if hasattr(stage_function, 'init'):
                stage_function.init(**kwargs)
            stage_function.stage_name = stage 
        self.initialized = True

        for stage in self.all_stages:
            get_queue(stage)
            get_queue('reply')
            stage_function = self.graph.nodes()[stage].get('function', PassThruStage())
            thread=threading.Thread(target=stage_function.run)
            thread.daemon = True
            thread.start()
            self.threads.append(thread)

        for i in range(4):
            reply_handler_thread = threading.Thread(target=self.reply_handler)
            reply_handler_thread.daemon = True
            reply_handler_thread.start()
            self.threads.append(reply_handler_thread)


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
                    get_queue(entry).put(message)

        else:
            # assume we have a single value to pump through the pipeline
            #logging.debug('other')
            # create the message envelopes and execute the pipeline 
            # from the entry nodes 
            message = create_new_message(value, sample_rate=self.sample_rate)
            for entry in self.entry_nodes:
                    get_queue(entry).put(message)

        return


    def close(self):

        if self.initialized:
            # call all the closes
            for stage in self.all_stages:
                stage_function = self.graph.nodes()[stage].get('function', PassThruStage())
                if hasattr(stage_function, 'close'):
                    stage_function.close()

        for stage in self.all_stages:
            get_queue(stage).put(None)
        get_queue('reply').put(None)



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
            yield prefix + pointer + child_node + " (version, executions)"
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

        
