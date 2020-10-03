import time, abc, logging

class Stage(abc.ABC):

    def __init__(self):
        self.input_record_count = 0
        self.output_record_count = 0
        self.first_seen = 0
        self.execution_time = 0
        self.first_run = True

    def init(self, **kwargs):
        pass

    def __call__(self, record):
        """
        Alias for execute.
        
        This has the instrumentation enabled on it.
        """
        if self.first_run:
            self.first_run = False
            self.first_seen = time.time()
            logging.debug('first run of: {}'.format(self.__class__.__name__))
        self.input_record_count += 1
        start_ns = time.time_ns()

        # the main processing payload
        results = self.execute(record)

        self.execution_time += time.time_ns() - start_ns
        for result in results or []:
            self.output_record_count += 1
            yield result

    @abc.abstractmethod
    def execute(self, record):
        """
        The payload, this must be overridden.
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