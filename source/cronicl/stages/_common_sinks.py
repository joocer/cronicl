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
    def execute(self, record):
        print('>>>', record)
        #yield record
        return None

#####################################################################

class FileSink(Stage):
    """
    Writes records to a file
    """
    def __init__(self, filename):
        self.filename = filename
        self.file = open(self.filename, 'w', encoding='utf-8')
        Stage.__init__(self)

#    def init(self, **kwargs):
#        self.filename = kwargs.get('filesink_target_file', '')
#        self.file = open(self.filename, 'w', encoding='utf-8')

    def execute(self, record):
        self.file.write("{}\n".format(str(record).rstrip('\n|\r')))
        return None

    def close(self):
        self.file.close()

#####################################################################

class NullSink(Stage):
    """
    Empty Sink
    """
    def execute(self, record):
        pass