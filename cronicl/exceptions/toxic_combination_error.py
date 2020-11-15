"""
Toxic Combination

Trigger/Dispatcher combinations that should never be seen,
even in development and test environments.
"""

from .cronicl_exception import CroniclException


class ToxicCombinationError(CroniclException):
    pass
