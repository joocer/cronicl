
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


def validate(dataset, validator):
    for row in dataset:
        validator.test(row)


def pass_through_validator(dataset, validator):
    for row in dataset:
        validator.test(row)
        yield row
        

class Validator(object):

    def __init__(self, schema, raise_errors = False, strict = False):
        self.schema = schema
        self.raise_errors = raise_errors
        self._validators = {
            'string'    : self._validate_string,
            'numeric'   : self._validate_numeric,
            'date'      : self._validate_date,
            'other'     : lambda x: True 
        }


    def test(self, dictionary):
        results = [ self._field_validator(key, value) for key, value in dictionary.items() ]
        return all(results)
        

    def _field_validator(self, key, value):
        """
        validator factory
        """
        rule = self.schema.get(key, 'other')
        splitted = rule.split(' ')
        validator = self._validators.get(splitted[0], lambda x: self._null_validator(x))

        return validator (value)


    def _validate_string(self, value):
        #('validate_string({})'.format(value))
        try:
            return str(value) == value
        except:
            return False


    def _validate_numeric(self, value):
        #print('validate_numeric({})'.format(value))
        try:
            test = float(value)
            return True
        except:
            return False


    def _validate_date(self, value):
        #print('validate_date({})'.format(value))
        try:
            if type(value).__name__ in ['datetime', 'date']:
                return True
            test = datetime.datetime.fromisoformat(value)
            return True
        except:
            return False


    def _null_validator(self, type):
        #print('null_validator({})'.format(type))
        warnings.warn('unknown field type: >>{}<<'.format(type))
        return True


def _test():
    val = validator({ 
        "name"   : "string",
        "age"    : "numeric",
        "height" : "numeric",
        "dob"    : "date",
        "title"  : "other"
        })
    print (val.test({ "name": "john", "age": "5", "dob": "2000-01-02", "height": 1.2, "title": "master", "spirit_animal": "penguin" }))

_test()