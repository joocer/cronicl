"""

Basic Validator

Supported Types:
    - string
    - numeric
    - date
    - other (default, always passes)

Schema is a dictionary
{
    "field_name": "type",
    "field_name": "type"
}


TODO:
    - raise errors on validation errors
    - regex validator
    - constraint additions (e.g. numeric [0 - 100] (ensures number is between 0 and 100))
    - fail on missing or excess fields 

"""

import re
from functools import reduce
import warnings
import datetime



class Validator(object):

    def __init__(self, schema, raise_errors = False, strict = False):
        self.schema = schema
        self.raise_errors = raise_errors
        self._validators = {
            'string'    : self._validate_string,
            'numeric'   : self._validate_numeric,
            'date'      : self._validate_date,
            'other'     : self._null_validator
        }
        self.validation_rules = { key: self._validators.get(rule, self._null_validator) for key, rule in schema.items() }


    def test(self, subject):
        return all( [ rule(subject.get(key)) for key, rule in self.validation_rules.items() ] )


    def __call__(self, subject):
        """
        Alias of .test()
        """
        return self.test(subject)


    def _validate_string(self, value):
        return type(value).__name__ == 'str'


    def _validate_numeric(self, value):
        try:
            test = float(value)
            return True
        except:
            return False


    def _validate_date(self, value):
        try:
            if type(value).__name__ in ['datetime', 'date', 'time']:
                return True
            test = datetime.datetime.fromisoformat(value)
            return True
        except:
            return False


    def _null_validator(self, type):
        return True