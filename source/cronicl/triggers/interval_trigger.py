
from .base_trigger import BasePollingTrigger
import datetime


class IntervalTrigger(BasePollingTrigger):
    """
    Trigger based on an interval
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.interval = kwargs.get('interval', 60)
        self._timestamp = kwargs.get('valid_from', datetime.datetime.min)
        if type(self.interval).__name__ == "int":
            # if a number, treat as seconds
            self.interval = datetime.timedelta(seconds=self.interval)
        elif not type(self.interval).__name__ == "timedelta":
            raise TypeError(
                "Interval must be either a number of seconds or a timedelta"
            )

    def nudge(self):
        """
        test if the condition to run has been met
        """
        if self._timestamp + self.interval < datetime.datetime.now():
            self._timestamp = datetime.datetime.now()
            self.on_event(self._timestamp)
            return True
        return False
