"""
Triggers hold the flow until some criteria is met

These are to be usually used at the start of a flow, however I should
write them so they can run mid-flow.


wait_for_file(filename) > file_to_json > filter_columns > save_to_file
"""

import datetime

statuses = ["Waiting", "Running", "Failed", "Complete"]


class BaseTrigger():

    def __init__(self, max_runs=1, valid_from=datetime.MINYEAR):
        """
        max runs < 0 = run until stopped
        """
        self.__state = "Waiting"

    def init(self, *args):
        pass

    def state(self):
        pass
        # waiting, running, complete, failed

    def nudge(self):
        pass
        # should I start running?


class IntervalTrigger(BaseTrigger):

    def __init__(self, interval, max_runs=-1, valid_from=datetime.MINYEAR):

        super().__init__()

        self.interval = None
        if type(interval).__name__ == 'int':
            # if a number, treat as seconds
            self.interval = datetime.timedelta(seconds=interval)
        if type(interval).__name__ == 'timedelta':
            self.interval = interval
        
        if self.interval is None:
            raise "Can't interpret interval parm"

    def state(self):
        pass
        # waiting, running, complete, failed

    def nudge(self):
        return False
        pass
        # should I start running?


class DateTrigger(BaseTrigger): pass
class FileWatchTrigger(BaseTrigger): pass
class InstantTrigger(BaseTrigger): pass
class WebHookTrigger(BaseTrigger): pass