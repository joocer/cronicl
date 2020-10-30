from ..models.BaseOperation import BaseOperation

class WriteToFileOperation(BaseOperation):
    """
    Writes records to a file
    """
    def __init__(self, filename):
        self.filename = filename
        self.file = open(self.filename, 'w', encoding='utf-8')
        # call the base initializer
        super.__init__()

    def execute(self, message):
        self.file.write("{}\n".format(str(message).rstrip('\n|\r')))
        return [message]

    def close(self):
        self.file.close()