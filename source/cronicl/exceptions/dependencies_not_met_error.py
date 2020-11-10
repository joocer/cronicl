"""
Dependencies not met

Call this when criteria for a method haven't been met.

For example an init hasn't been called first or a required method
doesn't exist on an object.
"""

from .croniclexception import CroniclException


class DependenciesNotMetError(CroniclException):
    pass
