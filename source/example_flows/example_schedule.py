
import datetime
import time
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
import cronicl
from cronicl.models import Scheduler
from cronicl.triggers import FileWatchTrigger

scheduler = Scheduler()
scheduler.add_flow(flow=None, trigger=FileWatchTrigger("this.file", interval=5))

while scheduler.running():
    time.sleep(60)
