from pipeline import Pipeline, Pipeline_Process_Base, json_to_string, string_to_json


filename = r'small.jsonl'


#####################################################################

class extract_followers(Pipeline_Process_Base):

    @Pipeline_Process_Base.Auditor()
    def execute(self, record):
        followers = int(record['followers'])
        return (followers, record['username'])

#####################################################################

class who_has_the_most(Pipeline_Process_Base):

    def init(self):
        self.max = 0
        self.user = ''

    @Pipeline_Process_Base.Auditor()
    def execute(self, record):
        if record[0] > self.max:
            self.max = record[0]
            self.user = record[1]
            return self.max, self.user


#####################################################################

def read_lines(file):
    """
    """
    tokens = 0
    line = file.readline()
    while line:
        yield line
        line = file.readline()

        tokens = tokens + 1
        #if tokens == 20000:
        #    return []
    print(tokens)
    return ['']



if __name__ == '__main__':

    #with timer.Timer() as t:

        with open(filename, 'r', encoding='utf-8') as file:

            pipeline = Pipeline()

            pipeline.add_process(string_to_json())
            pipeline.add_process(extract_followers())
            pipeline.add_process(who_has_the_most())
            pipeline.add_process(json_to_string())
            #pipeline.add(save_to_file('temp.jsonl'))

            results = map(pipeline.run, read_lines(file))

            for i in results:
                pass


            pipeline.audits()

