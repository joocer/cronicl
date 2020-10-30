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
import re
import hashlib

keys_to_sanitize  = ['password', 'pwd', '^pin$', '^pan$', '^cvc$']
values_to_santize = [   "[0-9]{16}",                           # very generic PAN detector
                        "[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{4}"  # very generic PAN detector
                    ]

def hash_it(value_to_hash):
    return hashlib.sha256(value_to_hash.encode()).hexdigest()[:16]

# refactored from a set of fors and ifs because apparently it was too
# hard too read.
def sanitize_record(record, message_id):
    # can't sanitize something that isn't a dict
    if not type(record).__name__ in ['dict', 'OrderedDict']:
        return record

    sanitized = record
    for key, value in record.items():
        value = str(value)
        value_to_hash = message_id + value

        key_expression_hits = [ hash_it(value_to_hash) for expression in keys_to_sanitize if re.match(expression, key, re.IGNORECASE) ]
        value_expression_hits = [ hash_it(value_to_hash) for expression in values_to_santize if re.match(expression, value, re.IGNORECASE) ]
        hashed_values = list(set(key_expression_hits + value_expression_hits))

        if len(hashed_values) == 1:
            sanitized[key] = hashed_values[0]
        
    return sanitized