import datetime
import os
import sys
import re
import pandas as pd
sys.path.insert(1, os.path.join(sys.path[0], '..'))
sys.path.insert(1, os.path.join(sys.path[0], '../..'))

from cronicl.models.Message import create_new_message
from  cronicl.models.BaseOperation import BaseOperation
import cronicl.operations


def test_operator(operator, payload):

    msg = create_new_message(payload)
    res = operator.execute(msg)

    print(type(res))


op = cronicl.operations.ValidatorOperation({"abd":"numeric"})

test_operator(op, {"abc":123})