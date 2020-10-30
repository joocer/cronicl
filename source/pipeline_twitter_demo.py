"""
Pipeline Demo - Twitter

A demonstration flow.

JSON File Pump
 └─ Data Validation
     └─ Extract Followers
         └─ Most Followers (verified)
             └─ Screen Sink

"""

import cronicl
from cronicl.utils import Timer
from cronicl.utils.Trace import get_tracer
from cronicl.models.Message import create_new_message 
import datasets.io
import logging, sys
import networkx as nx
import time

#logging.basicConfig( level=logging.DEBUG, format="%(created)-15s %(message)s")

class ExtractFollowersOperation(cronicl.BaseOperation):
    """
    Processing operation, unique to this pipeline.

    Create a class that inherits from Operation and override the 
    execute method. The method is passed a message object, this 
    has a payload attribute which is one record being pushed 
    through the pipeline.

    Perform the porcessing, update the payload and pass back a list
    of message objects, even if there's only one.
    """
    def execute(self, message):
        payload = message.payload

        followers = int(payload.get('followers', -1))
        verified = payload.get('user_verified', 'False') == 'True'
        result = { 
            "followers": followers, 
            "user": payload.get('username', ''), 
            "verified": verified }
        message.payload = result
        return [message]


class MostFollowersOperation(cronicl.BaseOperation):
    """
    Another processing operation unique to this pipeline.

    Includes an init method, this is called once when the pipeline
    is being initialized
    """

    def init(self, **kwargs):
        self.user = ''
        self.followers = 0


    def execute(self, message):
        """
        This operation will drop records, it does that by returning
        None in the place of messages.

        No return also works but returning None is more explicit.
        """
        payload = message.payload
        if payload.get('followers') > self.followers:
            self.followers = payload.get('followers')
            self.user = payload.get('user')
            return [message]
        else:
            return [None]


# The pipeline is defined as a networkx graph
dag = nx.DiGraph()

# The nodes prepresent the operations, each node can have the 
# following attributes set:
# - function : mandatory, sets the operation class 
# - threads : optional, sets the number of threads to allocate for
#       this operation, limited to 5 (default = 1). 
# - sample_rate : sets a sample rate for this specific operation,
#       this doesn't affect other sampling.
# - retry_count : number of times to retry failed operations, limited
#       to 10 (default = 0)
# - retry_delay : number of seconds to wait between retries, limited
#       to 300 seconds (default = 0) 
dag.add_node('Data Validation', function=cronicl.operations.ValidatorOperation({ "followers": "numeric", "username" : "string" }), threads=1)
dag.add_node('Extract Followers', function=ExtractFollowersOperation(), threads=1)
#dag.add_node('Collect Max Followers', function=cronicl.operations.MaxCollector(['user', 'followers']), retry_count=1)
dag.add_node('Most Followers (verified)', function=MostFollowersOperation())
dag.add_node('Screen Sink', function=cronicl.operations.WriteToScreenOperation(), retry_count=1, retry_delay=100)

# The edges represent the connections between the operations. Edges 
# have an optional 'filter' attribute which will filter records 
# being sent to the next operation.
dag.add_edge('Data Validation', 'Extract Followers')
dag.add_edge('Extract Followers', 'Most Followers (verified)', filter=lambda x: x.payload.get('verified') == True )
#dag.add_edge('Extract Followers', 'Collect Max Followers')
dag.add_edge('Most Followers (verified)', 'Screen Sink')


def main():
    # Tell the tracer to use the FileTracer, we don't use this 
    # because we're setting the sample rate to zero, this is just
    # to show how it is done.
    get_tracer().set_handler(cronicl.utils.Trace.FileTracer('cronicl_trace.log'))

    # create a pipeline, pass it the graph we created, set the
    # trace sampling off
    flow = cronicl.Pipeline(dag, sample_rate=0.0, enable_api=True, api_port=8001)

    # run the initialization routines, this also runs the operation
    # inits
    flow.init()

    # draw the pipeline
    flow.draw()

    # Timing isn't required, here to show it in use
    with Timer('pipeline'):

        # create a filereader - the chunker is a performance tweak
        file_reader = datasets.io.read_jsonl("small.jsonl", limit=1000)
        for chunk in datasets.io.generator_chunker(file_reader, 1000):
            # execute the flow for the chunk
            # execute can handle generators, lists or individual 
            # records
            flow.execute(chunk)

        # the pipeline runs across threads, we need to wait for
        # all the operations to finish, keep checking every second
        while flow.running():
            time.sleep(1)

    # close the pipeline and the tracer
    flow.close()
    get_tracer().close()

    # read the sensors on the operations, this tells us things like
    # - how many records they processed
    # - hom much time they were active for
    for sensor in flow.read_sensors():
        print(sensor)



if __name__ == "__main__":
    main()