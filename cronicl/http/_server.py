from flask import Flask


from ..models.queue import queue_sizes
from ..utils.threadlock import ThreadLock

_pipeline = None
app = Flask(__name__)


@app.route("/")
def main():
    with ThreadLock():
        connections = queue_sizes()
        stages = _pipeline.read_sensors()
        flow = _pipeline.flow()

    response = {"stages": stages, "connections": connections, "flow": flow}

    return response


def api_initializer(pipeline, port):
    global _pipeline
    _pipeline = pipeline
    app.run(host="0.0.0.0", port=port)
