"""
Flow Demo - Twitter

A demonstration flow.

JSON File Pump
 └─ Data Validation
     └─ Extract Followers
         └─ Most Followers (verified)
             └─ Screen Sink

"""


import networkx as nx
import time
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
import cronicl
from cronicl.utils import Timer
from cronicl.tracers import FileTracer, get_tracer
import datasets.io


class ExtractFollowersOperation(cronicl.BaseOperation):
    """
    Processing operation, unique to this flow.

    Create a class that inherits from Operation and override the
    execute method. The method is passed a message object, this
    has a payload attribute which is one record being pushed
    through the flow.

    Perform the porcessing, update the payload and pass back a list
    of message objects, even if there's only one.
    """

    def execute(self, message):
        payload = message.payload

        followers = int(payload.get("followers", -1))
        verified = payload.get("user_verified", "False") == "True"
        result = {
            "followers": followers,
            "user": payload.get("username", ""),
            "verified": verified,
        }
        message.payload = result
        return [message]


class MostFollowersOperation(cronicl.BaseOperation):
    """
    Another processing operation unique to this flow.

    Includes an init method, this is called once when the flow
    is being initialized
    """

    def init(self, **kwargs):
        self.user = ""
        self.followers = 0

    def execute(self, message):
        """
        This operation will drop records, it does that by returning
        None in the place of messages.

        No return also works but returning None is more explicit.
        """
        payload = message.payload
        if payload.get("followers") > self.followers:
            self.followers = payload.get("followers")
            self.user = payload.get("user")
            return [message]
        else:
            return [None]


"""
The nodes prepresent the operations, each node can have the
following attributes set:
 - function : mandatory, sets the operation class
 - concurrency : optional, sets the maximum number of concurrently
       running instances of this operation
       (default = 1, limited to 5)
 - sample_rate : sets a sample rate for this specific operation,
       this doesn't affect other sampling.
 - retry_count : number of times to retry failed operations
       (default = 0, limited to 10)
 - retry_delay : number of seconds to wait between retries
       (default = 0, limited to 300) 
"""

dag = nx.DiGraph()

dag.add_node(
    "Data Validation",
    function=cronicl.operators.ValidatorOperation(
        {"followers": "numeric", "username": "string"}
    ),
    concurrency=1,
)
dag.add_node("Extract Followers", function=ExtractFollowersOperation())
dag.add_node("Most Followers (verified)", function=MostFollowersOperation())
dag.add_node(
    "Screen Sink",
    function=cronicl.operators.WriteToScreenOperation(),
    retry_count=1,
    retry_delay=100,
)

# The edges represent the connections between the operations. Edges
# have an optional 'filter' attribute which will filter records
# being sent to the next operation.

dag.add_edge("Data Validation", "Extract Followers")
dag.add_edge(
    "Extract Followers",
    "Most Followers (verified)",
    filter=lambda x: x.payload.get("verified") == True,
)
dag.add_edge("Most Followers (verified)", "Screen Sink")

"""
This DAG could also be defined using the shorthand > notation. This
is quicker to define but options such as retries, connector filters
and threads are threading options are unavailable.

The > notation does not enable filters on connectors, this can be
added later with an explicit dag.add_edge call, but be aware that
> notation removes the ability to name nodes.


data_validation = cronicl.operations.ValidatorOperation({ "followers": "numeric", "username" : "string" })
extract_followers = ExtractFollowersOperation()
most_followers = MostFollowersOperation()
screen_sink = cronicl.operations.WriteToScreenOperation()

dag = data_validation > extract_followers > most_followers > screen_sink
"""


def main():
    # Tell the tracer to use the FileTracer, we don't use this
    # because we're setting the sample rate to zero, this is just
    # to show how it is done.
    get_tracer().set_handler(FileTracer("cronicl_trace.log"))

    # create a flow, pass it the graph we created, set the
    # trace sampling off
    flow = cronicl.Flow(dag, sample_rate=0.0)

    # run the initialization routines, this also runs the operation
    # inits
    flow.init()

    # draw the flow
    flow.draw()

    # Timing isn't required, here to show it in use
    with Timer("flow"):

        # create a filereader - the chunker is a performance tweak
        file_reader = datasets.io.read_jsonl("small.jsonl", limit=10)
        for chunk in datasets.io.generator_chunker(file_reader, 1000):
            # execute the flow for the chunk
            # execute can handle generators, lists or individual
            # records
            flow.execute(chunk)

        # the flow runs across threads, we need to wait for
        # all the operations to finish, keep checking every second
        while flow.running():
            time.sleep(1)

    # close the flow and the tracer
    flow.close()
    get_tracer().close()

    # read the sensors on the operations, this tells us things like
    # - how many records they processed
    # - hom much time they were active for
    for sensor in flow.read_sensors():
        print(sensor)


if __name__ == "__main__":
    main()
