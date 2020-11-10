"""
Base Tracer

Tracers are used to log information about messages as they pass
through pipelines.



Tracing is driven from the messages, which also contains a .trace 
boolean attribute. This code is just the code which writes the
trace to file or other logger.

Tracers are pluggable, you can write a new tracer by inheritting 
baseTracer and using .setHandler
"""

import abc


class BaseTracer(abc.ABC):
    """
    Base Class for Tracer
    """

    @abc.abstractmethod
    def emit(
        self,
        msg_id,
        execution_start,
        execution_duration,
        operation,
        version,
        child,
        initializer,
        record,
    ):

        raise NotImplementedError("This method must be overriden.")

    def close(self):
        pass  # placeholder


class __tracer(object):
    """
    Handles writing trace logs out.
    Implemented as a Singleton.
    """

    _instance = None
    tracer = None

    def set_handler(self, tracer):
        if not issubclass(tracer.__class__, BaseTracer):
            raise TypeError("Tracers must inherit from BaseTracer.")
        self.tracer = tracer

    def emit(
        self,
        msg_id,
        execution_start,
        execution_duration,
        operation,
        version,
        child,
        initializer,
        record,
    ):
        if self.tracer:
            self.tracer.emit(
                msg_id,
                execution_start,
                execution_duration,
                operation,
                version,
                child,
                initializer,
                record,
            )

    def close(self):
        if self.tracer:
            self.tracer.close()


def get_tracer():
    """
    Call this to get an instance of the tracer
    """
    if __tracer._instance is None:
        __tracer._instance = __tracer()
    return __tracer._instance
