from pipeline import Pipeline, PipelineProcessBase
from pipeline import *

import logging, sys
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format="%(created)f %(message)s")

filename = r'small.jsonl'


#####################################################################

class extract_followers(PipelineProcessBase):

    @PipelineProcessBase.Sensor()
    def execute(self, record):
        followers = int(record['followers'])
        yield (followers, record['username'])

#####################################################################

class who_has_the_most(PipelineProcessBase):

    def init(self):
        self.max = 0
        self.user = ''

    @PipelineProcessBase.Sensor()
    def execute(self, record):
        if record[0] > self.max:
            self.max = record[0]
            self.user = record[1]
            yield self.max, self.user


#####################################################################
import json

def read_lines(file):
    """
    """
    tokens = 0
    line = file.readline()
    while line:

        #yield json.loads(line)
        yield line
        line = file.readline()

        tokens = tokens + 1
        if tokens == 100000:
            return
    print(tokens)
    return


twitter_schema = {
    "followers": "numeric",
    "username" : "string"
}


if __name__ == '__main__':

    with timer.Timer() as t:

        with open(filename, 'r', encoding='utf-8') as file:

            pipeline = Pipeline()

            pipeline.add_process(string_to_json())
            pipeline.add_process(validate(twitter_schema))
            #pipeline.add_process(print_record())
            pipeline.add_process(extract_followers())    
            pipeline.add_process(who_has_the_most())
            pipeline.add_process(json_to_string())
            #pipeline.add_process(save_to_file('temp.jsonl'))
            pipeline.add_process(double())
            #pipeline.add_process(print_record())

            results = pipeline.load(read_lines(file))

            for i in results:
                pass
                #print(i)


            pipeline.sensors()
            pipeline.close()

