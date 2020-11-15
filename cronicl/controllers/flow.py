import logging
import threading
import networkx as nx

from ..operators import NullOperation
from ..models.message import create_new_message
from ..models.queue import get_queue, queues_empty
from ..utils import Signals
from ..exceptions import (
    ValidationError,
    DependenciesNotMetError,
    MissingInformationError,
)


class Flow(object):
    def __init__(self, dag=None, sample_rate=0.001, label=None):
        self.threads = []
        self.paths = {}
        self.graph = dag
        self.all_operations = self.graph.nodes()
        self.initialized = False
        self.label = label

        if not dag or type(dag).__name__ != "DiGraph":
            raise MissingInformationError(f"Flows must have a valid DAG")

        if not label:
            raise MissingInformationError("Flows must have a label")

        # tracing can be resource heavy, so we trace a sample
        # default sampling rateis 0.1% (one per thousand)
        self.sample_rate = sample_rate

        # get the entry nodes - the ones with 0 incoming nodes
        self.entry_nodes = [
            node for node in self.all_operations if len(dag.in_edges(node)) == 0
        ]

        # VALIDATE THE GRAPH
        # The pipeline can't be cyclic
        has_loop = True
        try:
            nx.find_cycle(dag, orientation="ignore")
        except nx.NetworkXNoCycle:
            has_loop = False
        if has_loop:
            raise ValidationError(
                "Pipeline must not be cyclic, if unsure do not have more than on incoming connection on any operations."
            )
        # Every operation node must have a function attribute
        if not all([dag.nodes()[node].get("function") for node in self.all_operations]):
            raise ValidationError(
                "All Operations in the Pipeline must have a 'function' attribute.\nIf all Operations have a 'function', check Connector definitions for errors in Operation names."
            )
        # Every object on the function attribute must have an execute method
        if not all(
            [
                hasattr(dag.nodes()[node]["function"], "execute")
                for node in self.all_operations
            ]
        ):
            raise ValidationError(
                "The object on all 'function' attributes in a Pipeline must have an 'execute' method"
            )
        logging.debug(
            "loaded a pipeline with {} operations, {} entry point(s)".format(
                len(self.all_operations), len(self.entry_nodes)
            )
        )

    def reply_handler(self):
        """
        Accept messages on the reply queue, replies attest the
        operation they have come from, we work out the next
        operations and put the message onto their queue.

        queue.get() is blocking so this should be run in a separate
        thread.

        This is called for every message in the system, so for
        performance it caches key information. Although not a pure
        function, it is deterministic and idempotent, which should
        make it thread-safe.
        """
        queue = get_queue("reply")
        response = queue.get()
        while not response == Signals.TERMINATE:
            respondent, message = response
            # If it's the first time we've seen this respondent,
            # cache its path.
            if not self.paths.get(respondent):
                self.paths[respondent] = []
                outgoing_edges = self.graph.out_edges(respondent, default=[])
                for this_operation, next_operation in outgoing_edges:
                    data_filter = self.graph.get_edge_data(
                        respondent, next_operation
                    ).get("filter", lambda x: True)
                    self.paths[respondent].append((next_operation, data_filter))
            for next_operation, data_filter in self.paths.get(respondent):
                if message and data_filter(message):
                    get_queue(next_operation).put(message, False)
            response = queue.get()
        # None is used to terminate the handler
        logging.debug("REPLY handler got TERM signal")

    def init(self, **kwargs):
        def clamp(value, low_bound, high_bound):
            if value <= low_bound:
                return low_bound
            if value >= high_bound:
                return high_bound
            return value

        # call all the operation inits, pass the kwargs
        for operation in self.all_operations:
            operation_node = self.graph.nodes()[operation]
            operation_function = operation_node.get("function", NullOperation())
            if hasattr(operation_function, "init"):
                operation_function.init(**kwargs)
            # operations need to be told their name
            operation_function.operation_name = operation
            operation_function.sample_rate = clamp(
                operation_node.get("sample_rate", 0), 0, 1
            )
            operation_function.retry_count = clamp(
                operation_node.get("retry_count", 0), 0, 10
            )
            operation_function.retry_delay = clamp(
                operation_node.get("retry_delay", 0), 0, 300
            )
        self.initialized = True

        for operation in self.all_operations:
            # Operations can define a number of threads to create.
            #
            # This is intended to be used by operations with IO, if
            # the operation is just working in memory,
            # multi-threading is not advised as locks are likely to
            # cause slow processing. It is important to remember,
            # Python does not concurrently run threads, one thread
            # runs whilst the other wait.
            thread_count = self.graph.nodes()[operation].get("concurrency", 1)
            # clamp the number of threads between 1 and 5
            thread_count = clamp(thread_count, 1, 5)

            operation_function = self.graph.nodes()[operation].get(
                "function", NullOperation()
            )
            for _ in range(thread_count):
                thread = threading.Thread(target=operation_function.run)
                thread.daemon = True
                thread.start()
                self.threads.append(thread)
        # Create multiple threads to handle replies
        # The reply_handler has no locks and testing has shown that
        # multiple handlers increases overall pipeline throughput
        for _ in range(2):
            reply_handler_thread = threading.Thread(target=self.reply_handler)
            reply_handler_thread.daemon = True
            reply_handler_thread.start()
            self.threads.append(reply_handler_thread)

    def execute(self, value):
        if not self.initialized:
            raise DependenciesNotMetError(
                "Pipeline's init method must be called before execute"
            )
        # if the value isn't iterable, put it in a list
        if not type(value).__name__ in ["generator", "list"]:
            value = [value]
        # Create the message envelopes and execute the pipeline from
        # the entry nodes
        for v in value:
            message = create_new_message(v, sample_rate=self.sample_rate)
            for entry in self.entry_nodes:
                get_queue(entry).put(message, False)

    def close(self):
        if self.initialized:
            # call all the closes
            for operation in self.all_operations:
                operation_function = self.graph.nodes()[operation].get(
                    "function", NullOperation()
                )
                if hasattr(operation_function, "close"):
                    operation_function.close()

    def running(self):
        return not queues_empty()

    def read_sensors(self):
        readings = []
        for operation in self.all_operations:
            readings.append(self.graph.nodes()[operation].get("function").read_sensor())
        return readings

    # adapted from https://stackoverflow.com/questions/9727673/list-directory-tree-structure-in-python
    def _tree(self, node, prefix=""):

        space = "    "
        branch = " │  "
        tee = " ├─ "
        last = " └─ "

        contents = [node[1] for node in self.graph.out_edges(node, default=[])]
        # contents each get pointers that are ├── with a final └── :
        pointers = [tee] * (len(contents) - 1) + [last]
        for pointer, child_node in zip(pointers, contents):
            yield prefix + pointer + child_node
            if (
                len(self.graph.out_edges(node, default=[])) > 0
            ):  # extend the prefix and recurse:
                extension = branch if pointer == tee else space
                # i.e. space because last, └── , above so no more |
                yield from self._tree(child_node, prefix=prefix + extension)

    def draw(self):
        print("Pipeline Entry")
        for entry in self.entry_nodes:
            print(" └─ {}".format(entry))
            t = self._tree(entry, "    ")
            print("\n".join(t))

    def flow(self):
        result = {}
        for operation in self.all_operations:
            contents = [node[1] for node in self.graph.out_edges(operation, default=[])]
            result[operation] = contents
        return result
