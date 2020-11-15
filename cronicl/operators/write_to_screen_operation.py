"""
Write to Screen

Simple operator which prints text to the screen.
"""

from .base_operation import BaseOperation


class WriteToScreenOperation(BaseOperation):
    """
    Displays a record to the screen
    """

    def execute(self, message):
        print(">>>", message)
        return [message]
