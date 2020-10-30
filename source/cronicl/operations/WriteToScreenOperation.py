from ..models.BaseOperation import BaseOperation

class WriteToScreenOperation(BaseOperation):
    """
    Displays a record to the screen
    """
    def execute(self, message):
        print('>>>', message)
        return [message]