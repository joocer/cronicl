"""
Test for Operator classes.

This test isn't comprehensive, passing this doesn't guarantee no
problems when running operators.

This doesn't test operator function, just conformance to a set 
of rules
"""

import datetime
import os
import sys
import re

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from cronicl.models.message import create_new_message
from cronicl.models import Message
from cronicl.operators import BaseOperation
import cronicl


def perform(operator, payload):

    # operator should inherit from baseoperator
    assert (isinstance(operator, BaseOperation)), "Operators must inherit from BaseOperation"

    message = create_new_message(payload)
    result = operator.execute(message)

    # result should be a list, the list should be None or a Message object
    assert (type(result).__name__ == 'list'), "Operators must return a list"

    # the list must be comprised of Message or None
    for item in result:
        assert(isinstance(item, Message) or item is None), "Each result must be a Message (or None)"


def test_null_operator():
    op = cronicl.operators.NullOperation()
    perform(op, {"abc": 123})

def test_validator_operator():
    op = cronicl.operators.ValidatorOperation({"abc": "numeric"})
    perform(op, {"abc": 123})

def test_write_to_screen_operator():
    op = cronicl.operators.WriteToScreenOperation()
    perform(op, {"abc": 123})


if __name__ == "__main__":
    print("local execution of operator test")
    test_null_operator()
    test_validator_operator()
    test_write_to_screen_operator()
