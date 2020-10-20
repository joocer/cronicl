"""
Pipeline Demo - Wait for a file and then print it line by line

A demonstration flow.
"""

import cronicl
import logging, sys
import networkx as nx
import time
import os.path

filename = 'temp.txt'

#logging.basicConfig( level=logging.DEBUG, format="%(created)-15s %(message)s")


# The pipeline is defined as a networkx graph
dag = nx.DiGraph()
dag.add_node('Screen Sink', function=cronicl.operations.ScreenSink())

def main():
    # create a pipeline
    flow = cronicl.Pipeline(dag, sample_rate=0)

    # run the initialization routines, this also runs the operations
    # inits
    flow.init()


    while not os.path.isfile(filename):
        time.sleep(1)

    file_reader = cronicl.datasets.io.read_file(filename)
    for chunk in cronicl.datasets.io.generator_chunker(file_reader, 1000):
        flow.execute(chunk)

    while flow.running():
        time.sleep(1)

    flow.close()

    for sensor in flow.read_sensors():
        print(sensor)

if __name__ == "__main__":
    main()