
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
            'other'     : self.always_true
        }


    def test(self, subject):
        results = [ self._field_validator(key, value) for key, value in subject.items() ]
        return all(results)


    def __call__(self, subject):
        return self.test(subject)


    def _field_validator(self, key, value):
        """
        validator factory
        """
        rule = self.schema.get(key, 'other')
        validator = self._validators.get(rule, self._null_validator)
        return validator(value)


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
        #print('null_validator({})'.format(type))
        warnings.warn('unknown field type: >>{}<<'.format(type))
        return True


    def always_true(self, x):
        return True


def _test():
    val = Validator({ 
        "name"   : "string",
        "age"    : "numeric",
        "height" : "numeric",
        "dob"    : "date",
        "title"  : "other"
        })
    print (val.test({ "name": "john", "age": "5", "dob": "2000-01-02", "height": 1.2, "title": "master", "spirit_animal": "penguin" }))

_test()