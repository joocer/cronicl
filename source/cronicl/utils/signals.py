"""
Signals are special messages which affect behaviour.

- TERMINATE queue handlers to stop processing new messages

"""


class Signals:

    TERMINATE = object()
