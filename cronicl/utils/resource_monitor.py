import time
import datetime
import psutil
import os
import threading

def _inner():
    pid = os.getpid()
    while 1:
        for proc in psutil.process_iter(attrs=['pid', 'cpu_percent', 'num_threads', 'memory_percent']):
            if proc.info.pop('pid') == pid:
                stats = proc.info
                stats['read_io'] =  proc.io_counters().read_bytes
                stats['write_io'] = proc.io_counters().write_bytes
        print(stats)
        time.sleep(10)

def monitor():
    thread = threading.Thread(target=_inner)
    thread.daemon = True
    thread.start()

_inner()

