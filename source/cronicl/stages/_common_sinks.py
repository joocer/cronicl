"""
Common Sinks

A set of prewritten data sinks for reuse.
"""

from ._stage import Stage

#####################################################################

class ScreenSink(Stage):
    """
    Displays a record to the screen
    """
    def execute(self, message):
        print('>>>', message)
        return [message]

#####################################################################

class FileSink(Stage):
    """
    Writes records to a file
    """
    def __init__(self, filename):
        self.filename = filename
        self.file = open(self.filename, 'w', encoding='utf-8')
        Stage.__init__(self)

    def execute(self, message):
        self.file.write("{}\n".format(str(message).rstrip('\n|\r')))
        return [message]

    def close(self):
        self.file.close()

#####################################################################

class NullSink(Stage):
    """
    Empty Sink
    """
    def execute(self, message):
        return [message]