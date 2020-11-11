"""
File Tracer
"""
from .base_tracer import BaseTracer
import datetime


class FileTracer(BaseTracer):
    def __init__(self, sink):
        self.file = open(sink, "a", encoding="utf8")

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
        entry = "[{}] id:{} operation:{:<24} version:{:<16} start:{:<22} duration:{:.10f} child:{} init:{} record:{}\n".format(
            datetime.datetime.now().isoformat(),
            msg_id,
            operation[:24],
            str(version)[:16],
            str(execution_start),
            execution_duration,
            child,
            initializer,
            record,
        )
        self.file.write(entry)

    def close(self):
        self.file.close()
