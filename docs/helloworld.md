

~~~~
"""
Hello, World!

A basic implementation of a cronicl pipeline.
"""
import cronicl
import time

class SayHelloOperation(cronicl.BaseOperation):

    def execute(self, message):
        message.payload = "Hello, World"
        return [message]

class PrintMessageOperation(cronicl.BaseOperation):

    def execute(self, message):
        print(message.payload) 
        return [message]       


dag = SayHelloOperation() > PrintMessageOperation()

flow = cronicl.Pipeline(dag, enable_api=False)
flow.init()

flow.execute(None)

while flow.running():
    time.sleep(1)
~~~~