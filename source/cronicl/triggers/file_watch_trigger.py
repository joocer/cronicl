from .base_trigger import BasePollingTrigger
import datetime
import pathlib


class FileWatchTrigger(BasePollingTrigger):
    """
    Watch for the presence of a file; the filename can contain
    markers for date formatting.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.filename = kwargs['filename']
        self.last_filename = None

    def nudge(self):
        # build filename, 
        filename = datetime.datetime.today().strftime(self.filename)
        # does the filename exist
        path = pathlib.Path(filename)
        if path.is_file() and path != self.last_filename:
            self.on_event(filename)
            self.last_filename = path
            return True
        return False
