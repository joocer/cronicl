"""
Trigger Termination

Used to signal the Trigger should Terminate
"""

from .croniclexception import CroniclException


class StopTrigger(CroniclException):
    pass
