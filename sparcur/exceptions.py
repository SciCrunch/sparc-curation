class SparCurError(Exception):
    """ base class for sparcur errors """


class MissingSecretError(SparCurError):
    """ key not in secrets """


class UnhandledTypeError(SparCurError):
    """ haven't dealt with this yet """


class PathExistsError(SparCurError):
    """ path exists so don't try to symlink """


class MetadataIdMismatchError(SparCurError):
    """ there is already cached metadata and id does not match """


class MetadataCorruptionError(SparCurError):
    """ there is already cached metadata and id does not match """


class NoFileIdError(SparCurError):
    """ no file_id """


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


class CommandTooLongError(SparCurError):
    """ not the best solution ... """


class NoRemoteImplementationError(SparCurError):
    """ prevent confusion between local path data and remote path data """


class NoRemoteMappingError(SparCurError):
    """ prevent confusion between local path data and remote path data """


class BootstrappingError(SparCurError):
    """ Something went wrong during a bootstrap """


class NotBootstrappingError(SparCurError):
    """ Trying to run bootstrapping only code outside of a bootstrap """


class NoSourcePathError(SparCurError):
    """ dictionary at some level is missing the expected key """


class TargetPathExistsError(SparCurError):
    """ when adding to a path if fail_on_exists is set raise this """


class EncodingError(SparCurError):
    """ Some encoding error has occured in a file """


class FileTypeError(SparCurError):
    """ File type is not allowed """


class NoDataError(SparCurError):
    """ There was no data in the file (not verified with stat)
        FIXME HACK workaround for bad handling of empty sheets in byCol """


class NoMetadataRetrievedError(SparCurError):
    """ we failed to retrieve metadata for a remote id """


class NoRemoteFileWithThatIdError(SparCurError):
    """ the file you are trying to reach has been disconnected """


class LengthMismatchError(SparCurError):
    """ lenghts of iterators for a zipeq do not match """


