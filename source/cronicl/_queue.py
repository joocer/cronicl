import queue
import logging
import re

__queues = {}

def get_queue(topic):
    """
    Call this to get an instance of the tracer
    """
    topic = re.sub('[^0-9a-zA-Z]+', '_', topic).lower().rstrip('_').lstrip('_')
    if topic not in __queues:
        new_queue = queue.SimpleQueue()
        __queues[topic] = new_queue
        logging.debug(f'CREATED NEW Q: {topic}')
    return __queues.get(topic)

def queue_sizes():
    response = {}
    for q in __queues:
        response[q] = __queues[q].qsize()
    return response

def queues_empty():
    print(queue_sizes())
    return all(__queues[q].empty() for q in __queues)
