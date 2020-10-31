"""
Test for Operator classes.

This test isn't comprehensive, passing this doesn't guarantee no
problems when running operators.


"""

import datetime
import os
import sys
import re
import pandas as pd
sys.path.insert(1, os.path.join(sys.path[0], '..'))
sys.path.insert(1, os.path.join(sys.path[0], '../..'))

from cronicl.models.message import create_new_message
from  cronicl.models import BaseOperation
import cronicl.operations


def test_operator(operator, payload):

    # operator should inherit from baseoperator

    msg = create_new_message(payload)
    res = operator.execute(msg)

    # result should be a list, the list should be None or a Message object
    print(type(res))


op = cronicl.operations.ValidatorOperation({"abc":"numeric"})

test_operator(op, {"abc":123})