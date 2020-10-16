import datetime, abc
try:
    from google.cloud import logging
except:
    pass

class _Trace(object):
    """
    Handles writing trace logs out.
    Implemented as a Singleton.
    """
    _instance = None
    def __init__(self):
        pass

    def setHandler(self, tracer):
        self.tracer = tracer

    def emit(self, msg_id, topic, stage, version, child, initializer, record):
        self.tracer.emit(msg_id, topic, stage, version, child, initializer, record)


def Trace():
    """
    Call this to get an instance of the tracer
    """
    if _Trace._instance is None:
        _Trace._instance = _Trace()
    return _Trace._instance


class baseTracer(object):
    """
    Base Class for Tracer
    """
    @abc.abstractmethod
    def emit(self, *args):
        pass


class NullTracer(baseTracer):
    """
    Just ignore everything
    """
    def emit(self, *args):
        pass


class FileTracer(baseTracer):
    """
    Write traces out to a file
    """
    def __init__(self, sink):
        self.file = open(sink, 'a', encoding='utf8')

    def emit(self, msg_id, topic, stage, version, child, initializer, ecord):
        entry = "{} id:{} topic:{:<16} stage:{:<24} version:{:<16} child:{} init:{} record:{}\n".format(
            datetime.datetime.now().isoformat(), 
            msg_id, 
            topic[:16], 
            stage[:24], 
            str(version)[:16],
            child, 
            initializer,
            record)
        self.file.write(entry)
   
   
class StackDriverTracer(baseTracer):
    """
    Write traces to Google StackDriver
    """
    def __init__(self, sink):
        self.logging_client = logging.Client()
        self.logger = self.logging_client.logger(sink)
    def emit(self, msg_id, topic, stage, version, child, initializer, record):
        entry = {
            "id": msg_id,
            "topic": topic,
            "stage": stage,
            "version": str(version)[:16],
            "child": child,
            "initializer": initializer,
            "record": record
        }
        self.logger.log_struct(entry)