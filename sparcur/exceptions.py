class SparCurError(Exception):
    """ base class for sparcur errors """

class UnhandledTypeError(SparCurError):
    """ haven't dealt with this yet """

class MetadataIdMismatchError(SparCurError):
    """ there is already cached metadata and id does not match """

class NotInProjectError(SparCurError):
    """fatal: not a spc directory {}"""
    def __init__(self, message=None):
        if message is None:
            more = '(or any of the parent directories)' # TODO filesystem boundaries ?
            self.message = self.__doc__.format(more)

class CommandTooLongError(Exception):
    """ not the best solution ... """


class NoRemoteImplementationError(Exception):
    """ prevent confusion between local path data and remote path data """


class NoRemoteMappingError(Exception):
    """ prevent confusion between local path data and remote path data """


