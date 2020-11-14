"""
Null Tracer

Don't Log Anything.
"""

from .base_tracer import BaseTracer


class NullTracer(BaseTracer):
    pass  # Just ignore everything
