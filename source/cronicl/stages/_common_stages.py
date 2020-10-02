from ._stage import Stage

# read file
# save to file

#####################################################################

class ScreenSink(Stage):
    """
    """
    @Stage.Sensor()
    def execute(self, record):
        print('>>>', record)
        yield None

#####################################################################

class FilePump(Stage):
    """
    """
    def init(self, **kwargs):
        self.input_file = kwargs.get('input_file', '')

    @Stage.Sensor()
    def execute(self, record):
        pass

    def close(self):
        pass

#####################################################################

class PassThruStage(Stage):
    """
    """
    @Stage.Sensor()
    def execute(self, record):
        yield record

#####################################################################
