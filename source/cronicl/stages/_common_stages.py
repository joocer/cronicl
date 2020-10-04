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
    def execute(self, record):
        yield record

#####################################################################

class ValidatorStage(Stage):
    """
    Validate against a schema.
    """
    def __init__(self, schema):
        self.validator = Validator(schema)
        self.invalid_records = 0
        Stage.__init__(self)

    def execute(self, record):
        valid = self.validator(record)
        if not valid:
            self.invalid_records += 1
        else:
            yield record

    def read_sensor(self):
        readings = Stage.read_sensor(self)
        readings['invalid_records'] = self.invalid_records
        return readings
