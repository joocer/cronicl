try:
    import ujson as json
except ImportError:
    import json
import logging
import abc
import time
import os, sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from datasets import Validator


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

            return inner_func


    def __init__(self, identifier=None):
        self.record_count = 0
        self.first_seen = 0
        self.execution_time = 0
    
    def init(self):
        pass

    @abc.abstractmethod
    def execute(self, record):
        pass

    def close(self):
        pass

    def read_sensor(self):
        return { 
            'process': self.__class__.__name__,
            'records': self.record_count,
            'execution_start': self.first_seen,
            'execution_time': self.execution_time / 1e9
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


class validate(PipelineProcessBase):
    """
    Validate against a schema.
    """
    def __init__(self, schema):
        self.validator = Validator(schema)
        self.invalid_records = 0
        PipelineProcessBase.__init__(self)


    @PipelineProcessBase.Sensor()
    def execute(self, record):
        valid = self.validator(record)
        if not valid:
            self.invalid_records += 1
        else:
            yield record


    def read_sensor(self):
        readings = PipelineProcessBase.read_sensor(self)
        readings['invalid_records'] = self.invalid_records
        return readings


class save_to_file(PipelineProcessBase):
    """
    Write records to a file.
    """
    def __init__(self, filename):
        # open file for writing
        self.file = open(filename, 'w', encoding='utf-8')
        # if we're defining an '__init__', we need to call the base
        # class' __init__ method
        PipelineProcessBase.__init__(self)

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