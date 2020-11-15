"""
Validation Error

Call this when a validation routine discovers invalid data.
"""

from .cronicl_exception import CroniclException


class ValidationError(CroniclException):
    pass
