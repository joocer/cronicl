"""
Pipeline Demo - Timer

Very simple flow, user a TimerPump to send a message approximately
every second and output the message to the screen.
"""

import cronicl
import logging, sys
import networkx as nx

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format="%(created)f %(message)s")

timed_dag = nx.DiGraph()
timed_dag.add_node('timer', function=cronicl.stages.TimerPump(10))
timed_dag.add_node('screen', function=cronicl.stages.ScreenSink())
timed_dag.add_edge('timer', 'screen')

flow = cronicl.Pipeline(timed_dag)
flow.init()
flow.draw()
flow.execute(None)
flow.close()