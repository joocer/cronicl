"""
Implements the logging parts of the tracer.

Tracing is driven from the messages, which also contains a .trace 
boolean attribute. This code is just the code which writes the
trace to file or other logger.

Tracers are pluggable, you can write a new tracer by inheritting 
baseTracer and using .setHandler
"""

import datetime, abc
try:
    from google.cloud import logging
except ImportError:
    pass # it's not there, so ignore

class _Trace(object):
    """
    Handles writing trace logs out.
    Implemented as a Singleton.
    """
    _instance = None
    def __init__(self):
        pass

    def set_handler(self, tracer):
        if not issubclass(tracer.__class__, BaseTracer):
            raise TypeError('Tracers must inherit from BaseTracer.')
        self.tracer = tracer

    def emit(self, msg_id, stage, version, child, initializer, record):
        self.tracer.emit(msg_id, stage, version, child, initializer, record)

    def close(self):
        self.tracer.close()


def get_tracer():
    """
    Call this to get an instance of the tracer
    """
    if _Trace._instance is None:
        _Trace._instance = _Trace()
    return _Trace._instance


class BaseTracer(object):
    """
    Base Class for Tracer
    """
    @abc.abstractmethod
    def emit(self, *args):
        pass
    def close(self):
        pass


class NullTracer(BaseTracer):
    """
    Just ignore everything
    """
    def emit(self, *args):
        pass


class FileTracer(BaseTracer):
    """
    Write traces out to a file
    """
    def __init__(self, sink):
        self.file = open(sink, 'a', encoding='utf8')

    def emit(self, msg_id, stage, version, child, initializer, record):
        entry = "{} id:{} stage:{:<24} version:{:<16} child:{} init:{} record:{}\n".format(
            datetime.datetime.now().isoformat(), 
            msg_id, 
            stage[:24], 
            str(version)[:16],
            child, 
            initializer,
            record)
        self.file.write(entry)
    
    def close(self):
        self.file.close()
   
   
class StackDriverTracer(BaseTracer):
    """
    Write traces to Google StackDriver
    """
    def __init__(self, sink):
        self.logging_client = logging.Client()
        self.logger = self.logging_client.logger(sink)
    def emit(self, msg_id, stage, version, child, initializer, record):
        entry = {
            "id": msg_id,
            "stage": stage,
            "version": str(version)[:16],
            "child": child,
            "initializer": initializer,
            "record": record
        }
        self.logger.log_struct(entry)