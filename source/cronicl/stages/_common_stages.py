"""
Common Stages

A set of prewritten stages for reuse.
"""

from ._stage import Stage
from ..datasets import Validator

#####################################################################

class PassThruStage(Stage):
    """
    Just passes values through.
    """
    def version(self):
        return 1

    def execute(self, message):
        yield message

#####################################################################

class ValidatorStage(Stage):
    """
    Validate against a schema.
    """
    def version(self):
        return 1
        
    def __init__(self, schema):
        self.validator = Validator(schema)
        self.invalid_records = 0
        Stage.__init__(self)

    def execute(self, message):
        valid = self.validator(message.payload)
        if not valid:
            self.invalid_records += 1
        else:
            yield message

    def read_sensor(self):
        readings = Stage.read_sensor(self)
        readings['invalid_records'] = self.invalid_records
        return readings
