try:
    import ujson as json
except ImportError:
    import json
import logging
import abc
import time


class PipelineProcessBase(abc.ABC):
    """
    Base class for Processes.

    Mostly empty methods to ensure they are present.
    """

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
                try:
                    if self.first_run:
                        self.first_run = False
                        self.parent = args[0]
                        self.parent.first_seen = time.time()
                   
                   # this is the execution payload
                    return_values = function(*args)

                    for return_value in return_values:
                        self.parent.record_count += 1
                        yield return_value
                except:
                    logging.error('an exception occurred which needs a better error message')
                    return

                self.parent.last_seen = time.time()

            return inner_func


    def __init__(self, identifier=None):
        self.record_count = 0
        self.first_seen = 0
        self.last_seen = 0
    
    def init(self):
        pass

    @abc.abstractmethod
    def execute(self, record):
        raise Exception('The base implementation of \'run\' has to be overridden')

    def close(self):
        pass

    def read_sensor(self):
        return { 
            'process': self.__class__.__name__,
            'records': self.record_count,
            'first_execution': self.first_seen,
            'last_execution': self.last_seen,
            'duration': self.last_seen - self.first_seen
        }


class string_to_json(PipelineProcessBase):
    """
    Convert string to JSON record.
    """
    @PipelineProcessBase.Sensor()
    def execute(self, record):
        yield json.loads(record)


class json_to_string(PipelineProcessBase):
    """
    Convert JSON records to strings.
    """
    @PipelineProcessBase.Sensor()
    def execute(self, record):
        yield json.dumps(record)


class save_to_file(PipelineProcessBase):
    """
    Write records to a file.
    """
    def __init__(self, filename):
        # open file for writing
        self.file = open(filename, 'w', encoding='utf-8')
        # if we're defining an '__init__', we need to call the base
        # class' __init__ method
        PipelineProcessBase.__init__(PipelineProcessBase)

    @PipelineProcessBase.Sensor()
    def execute(self, record):
        self.file.write("{}\n".format(str(record).rstrip('\n|\r')))
        yield record

    def close(self):
        # explicitly close the file
        self.file.close()


class print_record(PipelineProcessBase):
    """
    Writes records to the console.

    Intended for testing only.
    """
    @PipelineProcessBase.Sensor()
    def execute(self, record):
        print(record)
        yield record


class double(PipelineProcessBase):
    """
    Repeats a record.

    Intended for testing only.
    """
    @PipelineProcessBase.Sensor()
    def execute(self, record):
        yield record
        yield record