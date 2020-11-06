import abc
from ..exceptions import StopTrigger
import datetime
import time


class BaseTrigger(abc.ABC):
    """
    Base Trigger
    """

    def set_flow(self, flow):
        """
        Set the flow this trigger should feed data to
        """
        self.flow = flow

    @abc.abstractmethod
    def engage(self, flow):
        """
        'engage' is called when a trigger is loaded.

        This should start any listening activities - like
        subscribing to message queues.
        """
        raise NotImplementedError("'engage' must be overridden")

    def on_event(self, *args, **kwargs):
        """
        DO NOT OVERRIDE THIS METHOD
        """
        self.flow.execute(*args, **kwargs)


class BasePollingTrigger(BaseTrigger):
    """
    Base Polling Trigger

    parameters
    - interval: the number of seconds between polls
    - max_runs: the number of times to execute the flow from this
                trigger, -1 = run forever
    """

    def __init__(self, interval=60, max_runs=1):
        """
        max runs < 0 = run until stopped
        """
        self.__state = "Waiting"
        self.interval = interval
        self.max_runs = max_runs

    @abc.abstractmethod
    def nudge(self):
        """
        This must call on_event with the data to pass to the
        pipeline.

        This method shouldn't return anything.
        """
        raise NotImplementedError("'nudge' must be overridden")

    def engage(self):
        """
        Built in
        """
        while self.max_runs > 0:
            self.nudge()
            time.sleep(self.interval)

    def on_event(self, *args, **kwargs):
        """
        DO NOT OVERRIDE THIS METHOD
        """
        self.max_runs -= 1
        self.flow.execute(*args, **kwargs)
