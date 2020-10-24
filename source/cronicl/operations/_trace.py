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
    tracer = None

    def set_handler(self, tracer):
        if not issubclass(tracer.__class__, BaseTracer):
            raise TypeError('Tracers must inherit from BaseTracer.')
        self.tracer = tracer

    def emit(self, msg_id, operation, version, child, initializer, record):
        if self.tracer:
            self.tracer.emit(msg_id, operation, version, child, initializer, record)

    def close(self):
        if self.tracer:
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
        pass # to be overriden
    def close(self):
        pass # placeholder


class NullTracer(BaseTracer): pass # Just ignore everything


class FileTracer(BaseTracer):
    """
    Write traces out to a file
    """
    def __init__(self, sink):
        self.file = open(sink, 'a', encoding='utf8')

    def emit(self, msg_id, operation, version, child, initializer, record):
        entry = "{} id:{} operation:{:<24} version:{:<16} child:{} init:{} record:{}\n".format(
            datetime.datetime.now().isoformat(), 
            msg_id, 
            operation[:24], 
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
    def emit(self, msg_id, operation, version, child, initializer, record):
        entry = {
            "id": msg_id,
            "operation": operation,
            "version": str(version)[:16],
            "child": child,
            "initializer": initializer,
            "record": record
        }
        self.logger.log_struct(entry)