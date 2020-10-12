"""
Common Sinks

A set of prewritten data sinks for reuse.
"""

from ._stage import Stage
import warnings
try:
    from google.cloud import bigquery
except:
    pass

#####################################################################

class ScreenSink(Stage):
    """
    Displays a record to the screen
    """
    def execute(self, message):
        print('>>>', message)
        return [message]

#####################################################################

class FileSink(Stage):
    """
    Writes records to a file
    """
    def __init__(self, filename):
        self.filename = filename
        self.file = open(self.filename, 'w', encoding='utf-8')
        # call the base initializer
        Stage.__init__(self)

    def execute(self, message):
        self.file.write("{}\n".format(str(message).rstrip('\n|\r')))
        return [message]

    def close(self):
        self.file.close()

#####################################################################

class NullSink(Stage):
    """
    Empty Sink
    """
    def execute(self, message):
        return [message]

#####################################################################

class BigQuerySink(Stage):
    """
    Writes an entry to a GCS BigQuery Dataset
    """
    __attribute_override_warning = True

    def __init__(self, project=None, dataset=None, table=None):
        self.gcp_project = project
        self.bq_dataset = dataset
        self.bq_table = table
        # call the base initializer
        Stage.__init__(self)


    def execute(self, message):
        payload = message.payload

        project = message.attributes.get('project', self.gcp_project)
        dataset = message.attributes.get('dataset', self.bq_dataset)
        table   = message.attributes.get('table',   self.bq_table)

        inited_table = ".".join([self.gcp_project, self.bq_dataset, self.bq_table])
        my_table = ".".join([project, dataset, table])

        if self.__attribute_override_warning and (inited_table != my_table):
            warnings.warn('BigQuerySink is using project/dataset/table attributes from the message to override initialized values')

        client = bigquery.Client()
        table = client.get_table(my_table)
        row_to_insert = [payload]

        errors = client.insert_rows(table, row_to_insert) 

        if len(errors) > 0:
            for error in errors:
                print(error)
        
        return message