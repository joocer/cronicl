"""
Validation Error

Call this when a validation routine discovers invalid data.
"""

from .croniclexception import CroniclException


class ValidationError(CroniclException):
    pass
