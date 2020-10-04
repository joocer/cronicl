"""
Common Stages

A set of prewritten stages for reuse.
"""

from ._stage import Stage

#####################################################################

class PassThruStage(Stage):
    """
    Just passes values through.
    """
    def execute(self, record):
        yield record