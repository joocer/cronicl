"""
cron based trigger

Partial implementation of scheduled trigger using cron notation.
"""

from .base_trigger import BaseTrigger
import datetime
from datetime import timedelta
from ..util.cron import is_now
import time


def next_event(s):
    """
    Forecast the time of the next event based on a cron-like 
    speficification of the job schedule
    """
    dt = datetime.datetime.now()
    dt = dt.replace(second=0, microsecond=0)
    event = dt
    minute, hour, dom, month, dow = s.split(' ')

    if dow != '*':
        raise NotImplementedError("Event forecasting with DOW not supported")
    if month != '*':
        raise NotImplementedError("Event forecasting with Month not supported")
    if dom != '*':
        raise NotImplementedError("Event forecasting with DOM not supported")
    if minute != '*':
        event = event.replace(minute=int(minute))
        if event < dt:
            event = event + timedelta(hours=1)
    if hour != '*':
        event = event.replace(hour=int(hour))
        if event < dt:
            event = event + timedelta(days=1)

    return event


def seconds_until_next_event(s):
    event = next_event(s)
    return ((event - datetime.datetime.now()).total_seconds() // 1) + 1


class CronTrigger(BaseTrigger):
    """
    Trigger based on a schedule defined as per cron
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schedule = kwargs['schedule']

    def engage(self, logging):
        """
        Based on the main loop of cron:

        - Examine the task schedule, compute how far in the future it must run.
        - Sleep for that period of time.
        - On awakening and after verifying the correct time, execute the task.
        - Repeat
        """
        while True:
            if is_now(self.schedule):
                self.on_event(str(datetime.datetime.now()))
                time.sleep(60)
            seconds = seconds_until_next_event(self.schedule)
            if seconds < 1:
                print(F"negative sleep, waiting {seconds} seconds")
                time.sleep(-2)
            else:
                print(F"sleeping for {seconds} seconds")
                time.sleep(seconds)
