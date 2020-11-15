"""
Pipeline Demo - Wait for a file and then print it line by line

A demonstration flow.
"""

import networkx as nx
import time
import os.path
import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], ".."))
import cronicl
import cronicl.dispatchers
import cronicl.triggers
import datasets.io


def main():

    # The pipeline is defined as a networkx graph
    dag = nx.DiGraph()
    dag.add_node("Screen Sink", function=cronicl.operators.WriteToScreenOperation())
    # create a flow
    flow = cronicl.Flow(dag, sample_rate=0)
    # run the initialization routines, this also runs the operations
    # inits
    flow.init()

    filename = "temp.txt"
    dispatcher = cronicl.dispatchers.FlowDispatcher(flow=flow)
    trigger = cronicl.triggers.FileWatchTrigger(
        filename=filename, dispatcher=dispatcher
    )

    schedule = cronicl.Scheduler()
    schedule.add_trigger(trigger)

    while flow.running():
        print("beat")
        time.sleep(1)
    flow.close()

    for sensor in flow.read_sensors():
        print(sensor)


if __name__ == "__main__":
    main()
