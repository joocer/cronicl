from ..models.baseoperation import BaseOperation


class NullOperation(BaseOperation):
    """
    Does nothing.
    """

    def execute(self, message):
        return [message]
