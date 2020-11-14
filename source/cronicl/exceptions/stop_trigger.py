"""
Trigger Termination

Used to signal the Trigger should Terminate
"""

from .cronicl_exception import CroniclException


class StopTrigger(CroniclException):
    pass
