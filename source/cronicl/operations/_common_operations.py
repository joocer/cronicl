"""
Common Operations

A set of prewritten operations for reuse.
"""

from ._operation import Operation
from ..datasets import Validator

#####################################################################

class PassThruOperation(Operation):
    """
    Just passes values through.
    """
    def execute(self, message):
        return [message]

#####################################################################

class ValidatorOperation(Operation):
    """
    Validate against a schema.
    """
    def __init__(self, schema):
        self.validator = Validator(schema)
        self.invalid_records = 0
        Operation.__init__(self)

    def execute(self, message):
        valid = self.validator(message.payload)
        if not valid:
            self.invalid_records += 1
        else:
            return [message]

    def read_sensor(self):
        readings = Operation.read_sensor(self)
        readings['invalid_records'] = self.invalid_records
        return readings
