"""
Google StackDriver Tracer
"""
from ..models import BaseTracer

try:
    from google.cloud import logging
except ImportError:
    pass  # it's not there, so ignore


class StackDriverTracer(BaseTracer):
    """
    Write traces to Google StackDriver
    """

    def __init__(self, sink):
        self.logging_client = logging.Client()
        self.logger = self.logging_client.logger(sink)

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
        entry = {
            "id": msg_id,
            "operation": operation,
            "version": str(version)[:16],
            "start": execution_start,
            "duration": execution_duration,
            "child": child,
            "initializer": initializer,
            "record": record,
        }
        self.logger.log_struct(entry)
