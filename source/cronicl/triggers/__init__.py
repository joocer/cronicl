"""
Triggers hold the flow until some criteria is met

These are to be usually used at the start of a flow, however I should
write them so they can run mid-flow.


wait_for_file(filename) > file_to_json > filter_columns > save_to_file
"""

import datetime
import abc
import threading
from ..exceptions import StopTrigger

statuses = ["Waiting", "Ready", "Running", "Failed", "Complete"]


class BaseTrigger(abc.ABC):

    def __init__(self):
        pass

    def set_flow(self, flow):
        self.flow = flow

    @abc.abstractmethod
    def engage(self, flow):
        """
        'engage' is called when a trigger is loaded.
        """
        raise NotImplementedError("'engage' must be overridden")

    def on_event(self, *kwargs):
        self.flow.execute(*kwargs)

    def state(self):
        pass
        # waiting, running, complete, failed





class PubSubTrigger(BaseTrigger):
    pass


class BasePollingTrigger(BasePollingTrigger):

    def __init__(self, flow=None, interval=60, max_runs=1, valid_from=datetime.MINYEAR):
        """
        max runs < 0 = run until stopped
        """
        self.__state = "Waiting"
        self.interval = interval

    def init(self, *args):
        pass

    @abc.abstractmethod
    def nudge(self):
        pass
        # should I start running?
        # puts a message on the queue

    def engage(self):
        while True:
            if self.nudge():
                print("boom!")
                self.on_event()
            else:
                print("tick")
            time.sleep(self.interval)



    def callback(self):
        pass
        # set the status to Waiting, Failed or Complete


class IntervalTrigger(BaseTrigger):
    def __init__(self, interval=1, max_runs=-1, valid_from=datetime.datetime.min):

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
            self.on_event(context)
            return True
        return False


class DateTrigger(BaseTrigger):
    pass


class FileWatchTrigger(BaseTrigger):
    pass


class InstantTrigger(BaseTrigger):
    pass


class WebHookTrigger(BaseTrigger):
    pass


class GCSPubSubTrigger(BaseTrigger):
    pass


import time

t = IntervalTrigger(5)
while True:
    if t.nudge():
        print("boom!")
    else:
        print("tick")
    time.sleep(1)


def __schedule_thread_runner(flow, trigger):
    """
    The wrapper around triggers, this is blocking so should be
    run in a separate thread.

    - Memory and Keyboard Errors are bubbled.
    - StopTriggers gracefully
    - Other errors are ignored
    """
    trigger.flow = flow
    stop_trigger = False

    while not stop_trigger:
        try:
            trigger.engage()
        except KeyboardInterrupt:
            raise KeyboardInterrupt()
        except MemoryError:
            raise MemoryError()
        except StopTrigger:
            stop_trigger = True
        except:
            pass

class scheduler:
    def __init__(self):
        self.__threads = []
        self.__flows = []

    def add_flow(self, flow, trigger):

        api_thread = threading.Thread(target=__schedule_thread_runner, args=(self, flow, trigger))
        api_thread.daemon = True
        api_thread.start()
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


s = scheduler()
s.add_flow(flow=None, trigger=DateTrigger(max_runs=1, valid_from=datetime.datetime.today()))
s.add_flow(flow=None, trigger=FileWatchTrigger(filename="", max_runs=1))


while t.waiting():
    if t.nudge():
        something()
        # how does something known when it's complete?
    else:
        sleep(sleep)
