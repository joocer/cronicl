"""
Common Collectors

A set of collectors for reuse
"""

from ._operation import Operation
from .._signals import Signals
from ._messages import create_new_message

#####################################################################

class SumCollector(Operation):
    """
    Sums a set of fields in a message ignoring non-numeric values
    """
    def __init__(self, fields):
        self.fields = fields
        self.accumulated = { k:0 for k in fields }
        Operation.__init__(self)

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
                v = payload.get(k, 0)
                try:
                    self.accumulated[k] += float(v)
                except ValueError:
                    pass

        message.payload = payload
        return [message]

    def read_sensor(self):
        readings = Operation.read_sensor(self)
        readings = { **readings, **self.accumulated }
        return readings

class MaxCollector(Operation):
    """
    Returns max for each of a set of fields
    """
    def __init__(self, fields):
        self.fields = fields
        self.accumulated = { k:None for k in fields }
        Operation.__init__(self)

    def execute(self, message):
        payload = message.payload

        if payload == Signals.EMIT:
            payload = self.accumulated
            payload['source'] = self.operation_name
            return [create_new_message(payload)]
        elif payload == Signals.RESET:
            self.accumulated = { k:None for k in self.fields }
        else:
            for k in self.fields:
                v = payload.get(k)
                if not v:
                    pass
                if not self.accumulated[k]:
                    self.accumulated[k] = payload.get(k)
                elif v > self.accumulated[k]:
                    self.accumulated[k] = v

        message.payload = payload
        return [message]

    def read_sensor(self):
        readings = Operation.read_sensor(self)
        readings = { **readings, **self.accumulated }
        return readings

class MinCollector(Operation):
    """
    Returns max for each of a set of fields
    """
    def __init__(self, fields):
        self.fields = fields
        self.accumulated = { k:None for k in fields }
        Operation.__init__(self)

    def execute(self, message):
        payload = message.payload

        if payload == Signals.EMIT:
            payload = self.accumulated
            payload['source'] = self.operation_name
            return [create_new_message(payload)]
        elif payload == Signals.RESET:
            self.accumulated = { k:None for k in self.fields }
        else:
            for k in self.fields:
                v = payload.get(k)
                if not v:
                    pass
                if not self.accumulated[k]:
                    self.accumulated[k] = payload.get(k)
                elif v < self.accumulated[k]:
                    self.accumulated[k] = v

        message.payload = payload
        return [message]

    def read_sensor(self):
        readings = Operation.read_sensor(self)
        readings = { **readings, **self.accumulated }
        return readings