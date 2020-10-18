class ExceptionTemplate(Exception):
    def __call__(self, *args):
        return self.__class__(*(self.args + args))

class ValidationError(ExceptionTemplate): pass
class DependenciesNotMetError(ExceptionTemplate): pass