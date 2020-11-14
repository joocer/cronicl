import threading
import time
from ..exceptions import StopTrigger


def _schedule_thread_runner(trigger, logging):
    keep_running_trigger = True

    while keep_running_trigger:
        try:
            print('engage the trigger')
            trigger.engage(logging)
        except KeyboardInterrupt:
            raise KeyboardInterrupt()
        except MemoryError:
            raise MemoryError()
        except StopTrigger:
            keep_running_trigger = False
        time.sleep(5)
    print(f"Scheduler Thread {threading.current_thread().name} has terminated")


class Scheduler(object):

    def __init__(self):
        self._threads = []

    def add_trigger(self, trigger):
        event_thread = threading.Thread(
            target=_schedule_thread_runner, args=(trigger, self.logging)
        )
        event_thread.daemon = True
        event_thread.setName(F"scheduler:{trigger.__class__.__name__}")
        self._threads.append(event_thread)

    def execute(self):
        """
        Start each of the flow trigger threads
        """
        [t.start() for t in self._threads]

    def running(self):
        """
        Are any thread still running
        """
        return any([t.is_alive() for t in self._threads])

    def logging(self, **kwargs):
        print("logging: ", kwargs)

    def read_sensors(self):
        readings = {}
        readings['event_handlers'] = [t.read_sensors() for t in self._threads if t.is_alive()]
        return readings
