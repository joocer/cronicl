"""
Basic Validator

Supported Types:
    - string
    - numeric
    - date
    - boolean
    - other (default, always passes)

Schema is a dictionary
{
    "field_name": "type",
    "field_name": "type"
}
"""
import datetime


class Validator(object):

    def __init__(self, schema):
        self._validators = {
            'string'    : self._validate_string,
            'numeric'   : self._validate_numeric,
            'date'      : self._validate_date,
            'boolean'   : self._validate_boolean,
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


    def _validate_boolean(self, value):
        return str(value).lower in ['true', 'false', 'on', 'off', 'yes', 'no', '0', '1']


    def _validate_numeric(self, value):
        try:
            float(value)
            return True
        except ValueError:
            return False


    def _validate_date(self, value):
        try:
            if type(value).__name__ in ['datetime', 'date', 'time']:
                return True
            datetime.datetime.fromisoformat(value)
            return True
        except ValueError:
            return False


    def _null_validator(self, type):
        return True