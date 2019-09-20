from itertools import chain
from augpathlib.exceptions import *


class SparCurError(Exception):
    """ base class for sparcur errors """


class ValidationError(SparCurError):
    def __init__(self, errors):
        self.errors = errors

    def __repr__(self):
        msg = ', '.join([self._format_jsonschema_error(e) for e in self.errors])
        return self.__class__.__name__ + f'({msg})'

    def __str__(self):
        return repr(self)

    def json(self, pipeline_stage_name=None):
        """ update this to change how errors appear in the validation pipeline """
        skip = 'schema', 'instance', 'context'  # have to skip context because it has unserializable content
        return [{k:v if k not in skip else k + ' REMOVED'
                 for k, v in chain(e._contents().items(), (('pipeline_stage', pipeline_stage_name),))
                 # TODO see if it makes sense to drop these because the parser did know ...
                 if v and k not in skip}
                for e in self.errors]

    @staticmethod
    def _format_jsonschema_error(error):
        """Format a :py:class:`jsonschema.ValidationError` as a string."""
        if error.path:
            dotted_path = ".".join([str(c) for c in error.path])
            return "{path}: {message}".format(path=dotted_path, message=error.message)
        return error.message


class MissingSecretError(SparCurError):
    """ key not in secrets """


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


class NotInDatasetError(SparCurError):
    """ trying to run a comman on a dataset when not inside one """


class NotBootstrappingError(SparCurError):
    """ Trying to run bootstrapping only code outside of a bootstrap """


class EncodingError(SparCurError):
    """ Some encoding error has occured in a file """


class FileTypeError(SparCurError):
    """ File type is not allowed """


class NoDataError(SparCurError):
    """ There was no data in the file (not verified with stat)
        FIXME HACK workaround for bad handling of empty sheets in byCol """


class BadDataError(SparCurError):
    """ something went wrong """


class LengthMismatchError(SparCurError):
    """ lenghts of iterators for a zipeq do not match """


class LengthMismatchError(SparCurError):
    """ lenghts of iterators for a zipeq do not match """
