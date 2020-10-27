"""
Common Collectors

A set of collectors for reuse
"""

from ._operation import Operation
from ..utils import Signals
from ._messages import create_new_message
import abc

class BaseCollector(Operation):
    def __init__(self, fields):
        self.fields = fields
        self.accumulated = { k:0 for k in fields }
        Operation.__init__(self)

    def read_sensor(self):
        readings = Operation.read_sensor(self)
        readings = { **readings, **self.accumulated }
        return readings

    def execute(self, message):
        payload = message.payload

        if payload == Signals.EMIT:
            payload = self.accumulated
            payload['source'] = self.operation_name
            return [create_new_message(payload)]
        elif payload == Signals.RESET:
            self.accumulated = { k:0 for k in self.fields }
        else:
            for k in self.fields:
                v = payload.get(k, None)
                if v:
                    self.process(k, v)

        message.payload = payload
        return [message]

    @abc.abstractmethod
    def process(field, value): pass

#####################################################################

class SumCollector(BaseCollector):

    def process(self, field, value):
        try:
            self.accumulated[field] += float(value)
        except ValueError:
            pass


class MaxCollector(BaseCollector):

    def process(self, field, value):
        if not self.accumulated[field]:
            self.accumulated[field] = value
        elif value > self.accumulated[field]:
            self.accumulated[field] = value


class MinCollector(BaseCollector):

    def process(self, field, value):
        if not self.accumulated[field]:
            self.accumulated[field] = value
        elif value < self.accumulated[field]:
            self.accumulated[field] = value