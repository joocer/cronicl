"""
Test for Operator classes.

This test isn't comprehensive, passing this doesn't guarantee no
problems when running operators.


"""

import datetime
import os
import sys
import re

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from cronicl.models.message import create_new_message
from cronicl.operators import BaseOperation
import cronicl


def perform(operator, payload):

    # operator should inherit from baseoperator

    msg = create_new_message(payload)
    res = operator.execute(msg)

    # result should be a list, the list should be None or a Message object
    print(type(res))


def test_validator_operator():
    op = cronicl.operators.ValidatorOperation({"abc": "numeric"})
    perform(op, {"abc": 123})
