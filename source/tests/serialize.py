"""
Test to make sure that JSON serialization and deserialization creates
matching results
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
import cronicl
from cronicl.utils.serialization import dict_to_json, json_to_dict


TEST_DICT = {
    "string value": "string",
    "number value": 123,
    "boolean value": True,
    "list value": ["a", "b", "c"],
}


# Assert raises some quelity issues
def test(outcome, message=''):
    if not outcome:
        raise AssertionError(message)


def execute():

    serialized = dict_to_json(TEST_DICT)
    deserialized = json_to_dict(serialized)
    reserialized = dict_to_json(deserialized)

    test(
        TEST_DICT == deserialized
    ), "JSON Deserialization does not create matching results"
    test(
        serialized == reserialized
    ), "JSON Serialization does not creating matching results"


if __name__ == "__main__":
    print("local execution of serialization")
    execute()
