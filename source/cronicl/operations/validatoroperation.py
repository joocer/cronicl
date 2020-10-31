"""
Validator Operation
"""

from ..models.baseoperation import BaseOperation
from ..utils import Validator


class ValidatorOperation(BaseOperation):
    """
    Validate against a schema.
    """
    def __init__(self, schema):
        self.validator = Validator(schema)
        self.invalid_records = 0
        super().__init__()

    def execute(self, message):
        valid = self.validator(message.payload)
        if not valid:
            self.invalid_records += 1
        else:
            return [message]

    def read_sensor(self):
        """
        This isn't normally overriden.

        Obtain the data from the base class and extend it.
        """
        readings = BaseOperation.read_sensor(self)
        readings['invalid_records'] = self.invalid_records
        return readings
