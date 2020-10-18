from flask import Flask                                                         
import threading

from .._queue import queue_sizes

port = 8000
app = Flask(__name__)

@app.route("/")
def main():
    return queue_sizes()

def api_initializer():
    app.run(host='0.0.0.0', port=port)


