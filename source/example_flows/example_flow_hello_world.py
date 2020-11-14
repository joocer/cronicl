"""
Hello, World!

A basic implementation of a cronicl pipeline.
"""

import os
import sys
import time

sys.path.insert(1, os.path.join(sys.path[0], ".."))
import cronicl


# Custom Operation, says 'hello' to the name passed via the message
class SayHelloOperation(cronicl.BaseOperation):
    def execute(self, message):
        name = message.payload
        message.payload = f"Hello, {name}"
        return [message]


# Custom Operation, prints message to the screen
class PrintMessageOperation(cronicl.BaseOperation):
    def execute(self, message):
        print(message.payload)
        return [message]


# Define the DAG for this flow
dag = SayHelloOperation() > PrintMessageOperation()

# create a Flow instance and initialize
flow = cronicl.Flow(dag, enable_api=False)
flow.init()

# execute the flow, passing 'cronicl' as the data
flow.execute("cronicl")

# flows are asynchronous, wait for it to complete
while flow.running():
    time.sleep(1)

# close the flow
flow.close()
