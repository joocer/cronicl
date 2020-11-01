"""
Null Tracer

Don't Log Anything.
"""

from ..models import BaseTracer


class NullTracer(BaseTracer):
    pass  # Just ignore everything
