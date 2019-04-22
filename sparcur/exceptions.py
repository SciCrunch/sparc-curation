class SparCurError(Exception):
    """ base class for sparcur errors """


class UnhandledTypeError(SparCurError):
    """ haven't dealt with this yet """


class PathExistsError(SparCurError):
    """ path exists so don't try to symlink """


class MetadataIdMismatchError(SparCurError):
    """ there is already cached metadata and id does not match """


class MetadataCorruptionError(SparCurError):
    """ there is already cached metadata and id does not match """


class NoCachedMetadataError(SparCurError):
    """ there is no cached metadata """


class AlreadyInProjectError(SparCurError):
    """fatal: already in a spc project {}"""
    def __init__(self, message=None):
        if message is None:
            more = '(or any of the parent directories)' # TODO filesystem boundaries ?
            self.message = self.__doc__.format(more)


class NotInProjectError(SparCurError):
    """fatal: not a spc directory {}"""
    def __init__(self, message=None):
        if message is None:
            more = '(or any of the parent directories)' # TODO filesystem boundaries ?
            self.message = self.__doc__.format(more)


class ChecksumError(SparCurError):
    """ utoh """


class SizeError(SparCurError):
    """ really utoh """


class CommandTooLongError(Exception):
    """ not the best solution ... """


class NoRemoteImplementationError(Exception):
    """ prevent confusion between local path data and remote path data """


class NoRemoteMappingError(Exception):
    """ prevent confusion between local path data and remote path data """


class BootstrappingError(SparCurError):
    """ Something went wrong during a bootstrap """


class NotBootstrappingError(SparCurError):
    """ Trying to run bootstrapping only code outside of a bootstrap """
