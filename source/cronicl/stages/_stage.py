import time, abc, logging

class Stage(abc.ABC):

    class Sensor(object):
        """
        Decorator for Instrumentation.

        Count the number of records which have been returned by a 
        process. The record count is saved to the process.

        Records the first and last time the decorated method is 
        called.
        """
        def __init__(self):
            self.first_run = True
            self.parent = None

        def __call__(self, function):

            def inner_func(*args):
                #try:
                    if self.first_run:
                        self.first_run = False
                        self.parent = args[0]
                        self.parent.first_seen = time.time()

                    # count the input values
                    self.parent.input_record_count += 1
                   
                    # this is the execution payload
                    return_values = function(*args)

                    # count the output values
                    for return_value in return_values:
                        self.parent.output_record_count += 1
                        yield return_value
                #except:
                #    logging.error('an exception occurred which needs a better error message')
                #    return

            return inner_func


    def __init__(self):
        self.input_record_count = 0
        self.output_record_count = 0
        self.first_seen = 0
        self.execution_time = 0

    def init(self, **kwargs):
        pass

    def __call__(self, record):
        """
        Alias for execute
        """
        yield from self.execute(record)

    @abc.abstractmethod
    def execute(self, record):
        """
        The payload, this must be overridden
        """
        yield record

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