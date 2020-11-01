"""
Triggers hold the flow until some criteria is met

These are to be usually used at the start of a flow, however I should
write them so they can run mid-flow.


wait_for_file(filename) > file_to_json > filter_columns > save_to_file
"""

import datetime

statuses = ["Waiting", "Ready", "Running", "Failed", "Complete"]


class BaseTrigger:

    # on event is either called by nudge or by the event
    def on_event(self, context):
        pass


class BaseEventTrigger:
    # http listener
    # pubsub listener
    pass


class BasePollingTrigger:
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
        # puts a message on the queue

    def callback(self):
        pass
        # set the status to Waiting, Failed or Complete


class IntervalTrigger(BaseTrigger):
    def __init__(self, interval, max_runs=-1, valid_from=datetime.datetime.min):

        super().__init__()

        self.interval = None
        self.__timestamp = valid_from

        print(valid_from)

        if type(interval).__name__ == "int":
            # if a number, treat as seconds
            self.interval = datetime.timedelta(seconds=interval)
        elif type(interval).__name__ == "timedelta":
            self.interval = interval
        else:
            raise TypeError(
                "Interval must be either a number of seconds or a timedelta"
            )

    def state(self):
        pass
        # waiting, running, complete, failed

    def nudge(self):
        if self.__timestamp + self.interval < datetime.datetime.now():
            self.__timestamp = datetime.datetime.now()
            self.state = "Ready"
            return True
        return False

        self.on_event(context)


class DateTrigger(BaseTrigger):
    pass


class FileWatchTrigger(BaseTrigger):
    pass


class InstantTrigger(BaseTrigger):
    pass


class WebHookTrigger(BaseTrigger):
    pass


class GCS_PubSubTrigger(BaseTrigger):
    pass


import time

t = IntervalTrigger(5)
while True:
    if t.nudge():
        print("boom!")
    else:
        print("tick")
    time.sleep(1)


class scheduler:
    def __init__(self):
        self.__threads = []
        self.__flows = []

    def add_flow(self, flow, trigger):
        # create a thread for the trigger
        # when the trigger is true
        #   create a flow identifier - use this to log specific executions
        #   initiate the flow
        #   execute the flow (pass data from the trigger)
        # when the flow completes, inform the trigger
        pass

    def status(self):
        # for each of the flows
        #   get status information
        pass


while t.waiting():
    if t.nudge():
        something()
        # how does something known when it's complete?
    else:
        sleep(sleep)
