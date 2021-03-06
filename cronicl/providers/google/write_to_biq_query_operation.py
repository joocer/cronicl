from ..models.baseoperation import BaseOperation
import warnings

try:
    from google.cloud import bigquery
except ImportError:
    pass


class WriteToBiqQueryOperation(BaseOperation):
    """
    Writes an entry to a GCS BigQuery Dataset
    """

    __attribute_override_warning = True

    def __init__(self, project=None, dataset=None, table=None):
        self.gcp_project = project
        self.bq_dataset = dataset
        self.bq_table = table
        # call the base initializer
        super.__init__()

    def execute(self, message):
        payload = message.payload

        project = message.attributes.get("project", self.gcp_project)
        dataset = message.attributes.get("dataset", self.bq_dataset)
        table = message.attributes.get("table", self.bq_table)

        inited_table = ".".join([self.gcp_project, self.bq_dataset, self.bq_table])
        my_table = ".".join([project, dataset, table])

        if self.__attribute_override_warning and (inited_table != my_table):
            self.__attribute_override_warning = False
            warnings.warn(
                "BigQuerySink is using project/dataset/table attributes from the message to override initialized values"
            )

        client = bigquery.Client()
        table = client.get_table(my_table)
        row_to_insert = [payload]

        errors = client.insert_rows(table, row_to_insert)

        if len(errors) > 0:
            for error in errors:
                print(error)

        return [message]
