from ..models.basetrigger import BasePollingTrigger
import datetime
import pathlib


class FileWatchTrigger(BasePollingTrigger):

    def __init__(self, filename, **kwargs):
        self.filename = filename
        super().__init__(**kwargs)

    def nudge(self):
        # build filename
        filename = datetime.datetime.today().strftime(self.filename)
        # does the filename exist
        path = pathlib.Path(filename)
        if path.is_file():
            print("I'm here", path)
            # pass the filename to the flow
            self.on_event(filename)
        else:
            print("I'm not here", path)
