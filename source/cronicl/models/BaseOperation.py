"""
Operation is a base class which includes most of the heavy-lifting
for tracing and auditing.

There are three methods which are safe to override, 'init', 'close'
and 'execute', 'execute' must be overridden. 'execute' should
contain the processing logic, 'init' is intended for activities
like opening files and caching lookups, 'close' for any tidy-up
like closing files. 'execute' must return a list of messages.
"""

import time
import abc
import logging
import random
import inspect
import hashlib

from ..utils.Trace import get_tracer
from ..models import Message
from ..models.Queue import get_queue
from ..utils import ThreadLock, Signals

try:
    import ujson as json
except ImportError:
    import json


class BaseOperation(abc.ABC):

    def __init__(self):
        """
        IF OVERRIDEN, CALL THIS METHOD TOO.

        - like this super.__init__()
        """
        self.input_record_count = 0
        self.output_record_count = 0
        self.first_seen = None
        self.execution_time = 0
        self.my_version = None
        self.operation_name = ''
        self.errors = 0
        self.sample_rate = None
        self.retry_count = 0
        self.retry_delay = 0
        self.graph = None


    def init(self, **kwargs):
        """
        OVERRIDE IF REQUIRED

        Called once at the start of the pipeline.
        """
        pass


    def __call__(self, message):
        """
        DO NOT OVERRIDE THIS METHOD.
        
        This is where the auditting and tracing are implemented.
        """
        task_name = self.__class__.__name__

        if not self.first_seen:
            self.first_seen = time.time_ns()
            logging.debug('first run of: {}'.format(task_name))

        # deal with thread-unsafety
        with ThreadLock():
            self.input_record_count += 1

        traced = message.traced
        execution_start = time.time_ns()

        tries = self.retry_count + 1
        while tries > 0:
            # the main processing payload
            try:
                with ThreadLock():
                    results = self.execute(message)
                break # don't retry
            except KeyboardInterrupt:
                raise # don't count this as a processing error
            except:
                # don't reraise, count and continue
                with ThreadLock():
                    self.errors += 1
                if tries > 0:
                    time.sleep(self.retry_delay)
                tries -= 1   
                results = []

        # let's forgive the user for some things
        result_type = type(results)
        if isinstance(Message, result_type) or results is None:
            # the user forgot to put the result result in a list
            results = [results]
        elif result_type.__name__ == 'generator':
            # evaluate the full generator
            results = list(results)
        elif not result_type.__name__ == 'list':
            # we can't deal with everything, if we're here - fail
            raise TypeError('{} must \'return\' a list of messages (list can be 1 element long)'.format(task_name))

        execution_duration = time.time_ns() - execution_start
        # deal with thread-unsafety
        with ThreadLock():
            self.execution_time += execution_duration

        response = []
        # if the result is None this will fail
        for result in results or []:
            if result is not None:
                message.trace(operation=task_name, version=self.version(), child=result.id, execution_start=execution_start / 1e9, execution_duration=execution_duration / 1e9, force=self.force_trace())

                # messages inherit some values from their parent,
                # traced and initializer are required to be the
                # same as part of their core function
                result.traced = traced
                result.initializer = message.initializer

                # deal with thread-unsafety
                with ThreadLock():
                    self.output_record_count += 1

                response.append(result)

        if len(response) == 0:
            message.trace(operation=task_name, version=self.version(), child='00000000-0000-0000-0000-000000000000', execution_start=execution_start / 1e9, execution_duration=execution_duration / 1e9, force=self.force_trace())

        return response

    @abc.abstractmethod
    def execute(self, record):
        """
        MUST BE OVERRIDEN

        THIS SHOULD RETURN AN LIST OF MESSAGES

        Called once for every incoming record
        """
        raise NotImplementedError("This method must be overriden.")

    def version(self):
        """
        DO NOT OVERRIDE THIS METHOD.

        The version of the operation code, this is intended to 
        facilitate reproducability and auditability of the pipeline.

        The version is the last 8 characters of the hash of the 
        source code of the 'execute' method. This removes the need 
        for the developer to remember to increment a version 
        variable.

        Hashing isn't security sensitive here, it's to identify
        changes rather than protect information.
        """
        if not self.my_version:
            source = inspect.getsource(self.execute)
            full_hash = hashlib.sha224(source.encode())
            self.my_version = full_hash.hexdigest()[-8:]
        return self.my_version


    def close(self):
        """
        OVERRIDE IF REQUIRED

        Called once when pipeline has finished all records
        """
        pass


    def read_sensor(self):
        """
        IF OVERRIDEN, INCLUDE THIS INFORMATION TOO.

        This reads the auditting information from the operation.
        """
        return { 
            'process': self.__class__.__name__,
            'operation_name': self.operation_name,
            'version': self.version(),
            'input_records': self.input_record_count,
            'output_records': self.output_record_count,
            'errored_records': self.errors,
            'input_output_ratio': round(self.output_record_count / self.input_record_count, 4) if self.input_record_count else 0,
            'records_per_second': round(self.input_record_count / (self.execution_time / 1e9)) if self.execution_time else 0,
            'execution_start': self.first_seen,
            'execution_time': round(self.execution_time / 1e9, 4) if self.execution_time else 0
        }


    def run(self):
        """
        Method to run in a separate threat.
        """
        logging.debug(f"Thread running {self.operation_name} started")
        queue = get_queue(self.operation_name)
        # .get() is bocking, it will wait - which is okay if this
        # function is run in a thread
        message = queue.get()
        while not message == Signals.TERMINATE:
            results = self(message)
            for result in results:
                if result is not None:
                    reply_message = ( self.operation_name, result )
                    get_queue('reply').put(reply_message)
            message = queue.get()
        # None is used to exit the method
        logging.debug(f'TERM {self.operation_name}')


    def force_trace(self):
        """
        Operations have have higher tracing frequencies than the
        flow is set to; this doesn't affect the tracing at any
        other operation, preventing true tracing through the 
        pipeline but does increase observability at specific
        points.

        This does take into account if the message is already
        being traced.

        This was clamped between 0-1 when set.
        """
        if self.sample_rate:
            return random.randint(1, round(1/self.sample_rate)) == 1
        return False


    def __gt__(self, target):
        """
        Smart DAG builder. This allows simple DAGs to be defined
        using the following syntax:

        Op1 > Op2 > Op3
        """

        import networkx as nx
        if self.graph:
            graph = self.graph
        else:
            graph = nx.DiGraph()
        graph.add_node(target.__class__.__name__, function=target)
        graph.add_node(self.__class__.__name__, function=self)
        graph.add_edge(self.__class__.__name__, target.__class__.__name__)
        target.graph = graph
        del self.graph

        return graph