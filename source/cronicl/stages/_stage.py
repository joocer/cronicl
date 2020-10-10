import time, abc, logging, logging.handlers
try:
    import ujson as json
except ImportError:
    import json
from ._trace import Trace

class Stage(abc.ABC):

    def __init__(self):
        self.input_record_count = 0
        self.output_record_count = 0
        self.first_seen = 0
        self.execution_time = 0
        self.first_run = True

    def init(self, **kwargs):
        pass

    def __call__(self, message):
        """
        Alias for execute.
        
        This has the instrumentation enabled on it.
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
                message.trace(stage=task_name, version=self.version, child=result.id)
                result.traced = traced
                self.output_record_count += 1
                yield result

        if not has_results:
            message.trace(stage=task_name, version=self.version, child='00000000-0000-0000-0000-000000000000')

    @abc.abstractmethod
    def execute(self, record):
        """
        The payload, this must be overridden.
        """
        pass

    @abc.abstractmethod
    def version(self):
        """
        The version of the stage code, this is intended to facilitate
        reproducability and auditability of the pipeline.
        """
        pass

    def close(self):
        pass

    def read_sensor(self):
        return { 
            'process': self.__class__.__name__,
            'input_records': self.input_record_count,
            'output_records': self.output_record_count,
            'execution_start': self.first_seen,
            'execution_time': self.execution_time / 1e9
        }