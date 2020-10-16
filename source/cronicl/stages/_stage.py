import time, abc, logging
try:
    import ujson as json
except ImportError:
    import json
from ._trace import Trace
import inspect, hashlib
from .._queue import get_queue
import threading

class Stage(abc.ABC):

    def __init__(self):
        """
        IF OVERRIDEN, CALL THIS METHOD TOO.
        """
        self.input_record_count = 0
        self.output_record_count = 0
        self.first_seen = 0
        self.execution_time = 0
        self.first_run = True
        self.my_version = None
        self.stage_name = ''
        self.lock = threading.Lock()

    def init(self, stage_name='', **kwargs):
        """
        OVERRIDE IF REQUIRED

        Called once at the start of the pipeline.
        """
        pass

    def __call__(self, message):
        """
        DO NOT OVERRIDE THIS METHOD.
        
        This is where the auditting is implemented.
        """
        task_name = self.__class__.__name__

        if self.first_run:
            self.first_run = False
            self.first_seen = time.time()
            logging.debug('first run of: {}'.format(task_name))

        # deal with thread-unsafety
        self.lock.acquire() 
        self.input_record_count += 1
        self.lock.release()

        traced = message.traced
        start_ns = time.time_ns()

        # the main processing payload
        results = self.execute(message)

        return_type = type(results).__name__
        if return_type == 'generator' and not return_type == 'list':
            raise TypeError('{} must \'return\' a list of messages (list can be 1 element long)'.format(task_name))

        # deal with thread-unsafety
        self.lock.acquire() 
        self.execution_time += time.time_ns() - start_ns
        self.lock.release()

        response = []
        # if the result is None this will fail
        for result in results or []:
            if result is not None:
                message.trace(stage=task_name, version=self.version(), child=result.id)

                # messages inherit some values from their parent,
                # traced and initializer are required to be the
                # same as part of their core function
                result.traced = traced
                result.initializer = message.initializer

                # deal with thread-unsafety
                self.lock.acquire() 
                self.output_record_count += 1
                self.lock.release()

                response.append(result)

        if len(response) == 0:
            message.trace(stage=task_name, version=self.version(), child='00000000-0000-0000-0000-000000000000')

        return response

    @abc.abstractmethod
    def execute(self, record):
        """
        MUST BE OVERRIDEN

        THIS SHOULD RETURN AN ARRAY OF RESULTS

        Called once for every incoming record
        """
        pass

    def version(self):
        """
        DO NOT OVERRIDE THIS METHOD.

        The version of the stage code, this is intended to facilitate
        reproducability and auditability of the pipeline.

        The version is the last 8 characters of the hash of the 
        source code of the 'execute' method. This removes the need 
        for the developer to remember to increment a version 
        variable.
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
        """
        return { 
            'process': self.__class__.__name__,
            'version': self.version(),
            'input_records': self.input_record_count,
            'output_records': self.output_record_count,
            'execution_start': self.first_seen,
            'execution_time': self.execution_time / 1e9
        }


    def run(self):
        """
        Method to run in a separate threat.
        """
        logging.debug(f"Thread running {self.stage_name} started")
        queue = get_queue(self.stage_name)
        # .get() is bocking, it will wait - which is okay if this
        # function is run in a thread
        message = queue.get()
        while message:
            results = self(message)
            for result in results:
                if result is not None:
                    reply_message = ( self.stage_name, result )
                    get_queue('reply').put(reply_message)
            message = queue.get()
        # None is used to exit the method
        logging.debug(f'TERM {self.stage_name}')

