"""
Common Pumps

A set of prewritten data pumps for reuse.

Pumps generate data to be put through the pipeline.
"""

from ._stage import Stage
try:
    import ujson as json
except ImportError:
    import json

#####################################################################

class FilePump(Stage):
    """
    Reads records from a file
    """
    def execute(self, record):
        """
        Note that this will be instantiated once and read the
        file to the end.
        """
        with open(record, 'r', encoding='utf-8') as file:
            row = file.readline()
            while row:
                yield row
                row = file.readline()

#####################################################################

class JSONLFilePump(Stage):
    """
    Reads records from a file
    """
    def execute(self, record):
        """
        Note that this will be instantiated once and read the
        file to the end.
        """

        limit = -1

        with open(record, 'r', encoding='utf-8') as file:
            row = file.readline()
            while row:

                yield json.loads(row)
                row = file.readline()

                limit -= 1
                if limit == 0:
                    row = None



#####################################################################