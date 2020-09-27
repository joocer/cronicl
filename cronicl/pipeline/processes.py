try:
    import ujson as json
except ImportError:
    import json
import logging

class Pipeline_Process_Base(object):

    # this will automatically count records out
    class Auditor(object):
        def __init__(self):
            self.first_run = True
            self.parent = None
        def __call__(self, function):
            def inner_func(*args):
                try:
                    ret = function(*args)
                    if self.first_run:
                        self.first_run = False
                        self.parent = args[0]
                    self.parent.record_count += 1
                    return ret
                except:
                    logging.error('an exception occurred which needs a better error message')
                    return None
            return inner_func

    def __init__(self):
        self.record_count = 0

    def init(self):
        pass

    def execute(self, record):
        raise Exception('The base implementation of \'run\' has to be overridden')

    def get_audit_detatils(self):
        return { 'name': self.__class__.__name__,
            'records': self.record_count
        }

#####################################################################

class string_to_json(Pipeline_Process_Base):

    @Pipeline_Process_Base.Auditor()
    def execute(self, record):
        return json.loads(record)

#####################################################################

class json_to_string(Pipeline_Process_Base):

    @Pipeline_Process_Base.Auditor()
    def execute(self, record):
        return json.dumps(record)

#####################################################################

class save_to_file(Pipeline_Process_Base):

    def __init__(self, filename):
        self.file = open(filename, 'w')
        Pipeline_Process_Base.__init__(Pipeline_Process_Base)

    @Pipeline_Process_Base.Auditor()
    def execute(self, record):
        self.file.write("{}\n".format(record))
        return record
