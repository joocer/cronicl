"""
Queues are used to hold messages between operations. 

Queues allow the operations to: 
    a) be loosely coupled and 
    b) run in separate threads
"""

import queue, logging, re

__queues = {}

def get_queue(topic):
    """
    Call this to get an instance of the queue list
    """
    topic = re.sub('[^0-9a-zA-Z]+', '_', topic).lower().rstrip('_').lstrip('_')
    if topic not in __queues:
        new_queue = queue.SimpleQueue()
        __queues[topic] = new_queue
        logging.debug(f'Created new queue: {topic}')
    return __queues.get(topic)


def queue_sizes():
    """
    Return the sizes of all of the queues
    """
    response = {}
    for q in __queues:
        response[q] = __queues[q].qsize()
    return response


def queues_empty():
    """
    Returns True when all queues are empty
    """
    return all(__queues[q].empty() for q in __queues)
