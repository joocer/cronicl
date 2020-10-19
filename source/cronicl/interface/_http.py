from flask import Flask                                                         
import threading

from .._queue import queue_sizes

port = 8000
_pipeline = None
app = Flask(__name__)

@app.route("/")
def main():
    lock = threading.Lock()
    lock.acquire()
    connections = queue_sizes()
    stages = _pipeline.read_sensors()
    flow = _pipeline.flow()
    lock.release()

    response = { "stages" : stages, "connections" : connections, "flow" : flow }

    return response

def api_initializer(pipeline):
    global _pipeline
    _pipeline = pipeline
    app.run(host='0.0.0.0', port=port)


