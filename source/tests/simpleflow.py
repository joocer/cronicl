"""
Sanity Test

Can a basic flow execute without error?

This is a variation of the "Hello, World" example
"""

import os
import sys
import time
sys.path.insert(1, os.path.join(sys.path[0], '..'))
import cronicl


class SayHelloOperation(cronicl.BaseOperation):

    def execute(self, message):
        message.payload = "Hello, World"
        return [message]


class PrintMessageOperation(cronicl.BaseOperation):

    def execute(self, message):
        print(message.payload)
        return [message] 


def execute():
    dag = SayHelloOperation() > PrintMessageOperation()
    flow = cronicl.Pipeline(dag, enable_api=False)
    flow.init()
    flow.execute(None)

    while flow.running():
        time.sleep(1)

    return True