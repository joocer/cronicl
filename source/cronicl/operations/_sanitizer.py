"""
Record sanitizer.

Remove sensitive data from records before saving to external logs.

Note that the value is hashed using (SHA256) and the first 16 
characters of the hexencoded hash are presented. This information
allows values to be traced without disclosing the actual value. 

The Sanitizer can only sanitize dictionaries, it doesn't
sanitize strings, which could contain sensitive information

We use the message id as a salt to further protect sensitive 
information.
"""
import re, hashlib

keys_to_sanitize  = ['password', 'pwd', '^pin$', '^pan$', 'cvc']
values_to_santize = [   "[0-9]{16}",                           # very generic PAN detector
                        "[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{4}"  # very generic PAN detector
                    ]

def sanitize_record(record, message_id):
    if type(record).__name__ in ['dict', 'OrderedDict']:
        sanitized = { }
        for key, v in record.items():
            v = str(v)
            value_to_save = record.get(key, '')
            for key_regex in keys_to_sanitize:
                if re.match(key_regex, key, re.IGNORECASE):
                    value_to_hash = message_id + v
                    value_to_save = hashlib.sha256(value_to_hash.encode()).hexdigest()[:16]
            for value_regex in values_to_santize:
                if re.match(value_regex, v, re.IGNORECASE):
                    value_to_hash = message_id + v
                    value_to_save = hashlib.sha256(value_to_hash.encode()).hexdigest()[:16]
            sanitized[key] = value_to_save
        return sanitized
    else:
        # we can't sanitize things that aren't dicts
        return record