from .base_operation import BaseOperation


class NullOperation(BaseOperation):
    """
    Does nothing.
    """

    def execute(self, message):
        return [message]
