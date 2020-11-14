"""
Sanity Test

Can a basic flow execute without error?

This is a variation of the "Hello, World" example
"""

import os
import sys
import time

sys.path.insert(1, os.path.join(sys.path[0], ".."))
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
    flow = cronicl.Flow(dag=dag, label="test flow", sample_rate=0)
    flow.init()
    flow.execute(None)

    while flow.running():
        time.sleep(1)
    readings = flow.read_sensors()

    for reading in readings:
        assert reading["input_records"] == 1, "Input Records reading not 1"
        assert reading["output_records"] == 1, "Output Records reading not 1"
        assert reading["errored_records"] == 0, "Errored Records reading not 0"
        assert reading["input_output_ratio"] == 1, "Input:Output not 1"


if __name__ == "__main__":
    print("local execution of simple flow")
    execute()
