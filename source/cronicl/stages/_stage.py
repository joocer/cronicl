import time, abc, logging, logging.handlers
try:
    import ujson as json
except ImportError:
    import json
from ._trace import Trace
import inspect, hashlib

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

    def init(self, **kwargs):
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
        self.input_record_count += 1

        traced = message.traced
        start_ns = time.time_ns()

        # the main processing payload
        results = self.execute(message)

        self.execution_time += time.time_ns() - start_ns

        has_results = False
        # if the result is None this will fail
        for result in results or []:
            if result is not None:
                has_results = True
                message.trace(stage=task_name, version=self.version(), child=result.id)
                result.traced = traced
                self.output_record_count += 1
                yield result

        if not has_results:
            message.trace(stage=task_name, version=self.version(), child='00000000-0000-0000-0000-000000000000')

    @abc.abstractmethod
    def execute(self, record):
        """
        MUST BE OVERRIDEN

        THIS SHOULD yield ITS RESULTS

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