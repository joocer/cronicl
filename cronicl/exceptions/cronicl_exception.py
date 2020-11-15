"""
Base for new exception types.
"""


class CroniclException(Exception):
    def __call__(self, *args):
        return self.__class__(*(self.args + args))
