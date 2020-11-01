"""
Validation Error

Call this when a validation routine discovers invalud data.
"""

from .croniclexception import CroniclException


class ValidationError(CroniclException):
    pass
