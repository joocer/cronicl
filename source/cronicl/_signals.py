"""
Signals are special messages which affect behaviour.

- TERMINATE queue handlers to stop processing new messages
- EMIT      collector operations should show their current value, 
            this does not mean the collector should reset
- RESET     collector operations should reset their value and start
            collecting again
"""

class Signals:

    TERMINATE = object()
    EMIT = object()
    RESET = object()

    

