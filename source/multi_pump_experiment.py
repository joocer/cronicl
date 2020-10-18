"""
Pipeline Demo - Timer

Very simple flow, user a TimerPump to send a message approximately
every second and output the message to the screen.
"""

import cronicl
import logging, sys
import networkx as nx

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format="%(created)f %(message)s")

class RoundTheNumberStage(cronicl.stages.Stage):

    def execute(self, record):
        yield round(record.payload)

#timed_dag = nx.DiGraph()
#timed_dag.add_node('timer', function=cronicl.stages.TimerPump(5))
#timed_dag.add_node('round', function=RoundTheNumberStage())
#timed_dag.add_node('screen', function=cronicl.stages.ScreenSink())
#timed_dag.add_edge('timer', 'round')
#timed_dag.add_edge('round', 'screen')

#flow = cronicl.Pipeline(timed_dag)
#flow.draw()
#flow.execute(None)




with cronicl.timer.Timer() as t:
    f=open('test1.txt', 'w')
    for i in range(1000):
        f.write('\n')
    f.close()

with cronicl.timer.Timer() as t:
    for i in range(1000):
        f=open('test2.txt', 'a')
        f.write('\n')
        f.close()
    
with cronicl.timer.Timer() as t:
    for i in range(1000):
        f=open('test2.txt', 'a')
        print('', file=f)
        f.close()