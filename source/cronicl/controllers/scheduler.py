import threading
import time
from ..exceptions import StopTrigger


def _schedule_thread_runner(flow, trigger):
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
        # don't form a tight loop, slow it down
        time.sleep(5)


class Scheduler(object):
    """
    Scheduler allows multiple pipelines to be run.

    Scheduled tasks must have a trigger, this trigger will acquire
    the initializing data for the flow.
    """
    def __init__(self):
        self.__threads = []

    def add_flow(self, flow, trigger):
        api_thread = threading.Thread(
            target=_schedule_thread_runner,
            args=(flow, trigger))
        api_thread.daemon = True
        api_thread.start()

    def running(self):
        """
        Are any thread still running
        """
        print('Am I running?')
        return all([not t.active for t in self.__threads])
        