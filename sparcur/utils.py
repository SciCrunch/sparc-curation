import io
import logging
from idlib.utils import log as _ilog
from augpathlib.utils import log as _alog
from pyontutils.utils import makeSimpleLogger, python_identifier  # FIXME update imports
from pyontutils.utils import TZLOCAL, utcnowtz, isoformat, isoformat_safe
from sparcur.config import config


log = makeSimpleLogger('sparcur')
logd = log.getChild('data')
loge = log.getChild('export')

# set augpathlib log format to pyontutils (also sets all child logs)
_alog.removeHandler(_alog.handlers[0])
_alog.addHandler(log.handlers[0])
# idlib logs TODO move to pyontutils probably?
_ilog.removeHandler(_alog.handlers[0])
_ilog.addHandler(log.handlers[0])


class GetTimeNow:
    def __init__(self):
        self._start_time = utcnowtz()
        self._start_local_tz = TZLOCAL()  # usually PST PDT

    @property
    def _start_time_local(self):
        return self._start_time.astimezone(self._start_local_tz)

    @property
    def START_TIMESTAMP(self):
        return isoformat(self._start_time)

    @property
    def START_TIMESTAMP_SAFE(self):
        return isoformat_safe(self._start_time)

    @property
    def START_TIMESTAMP_LOCAL(self):
        return isoformat(self._start_time_local)

    @property
    def START_TIMESTAMP_LOCAL_SAFE(self):
        return isoformat_safe(self._start_time_local)


class SimpleFileHandler:
    _FIRST = object()
    def __init__(self, log_file_path, *logs, mimic=_FIRST):
        self.log_file_handler = logging.FileHandler(log_file_path.as_posix())
        if mimic is self._FIRST and logs:
            self.mimic(logs[0])
        elif mimic:
            self.mimic(mimic)

        for log in logs:
            self(log)

    def __call__(self, *logs_to_handle):
        for log in logs_to_handle:
            log.addHandler(self.log_file_handler)

    def mimic(self, log):
        self.log_file_handler.setFormatter(log.handlers[0].formatter)


def bind_file_handler(log_file):
    # FIXME the this does not work with joblib at the moment
    from idlib.utils import log as idlog
    from protcur.core import log as prlog
    from orthauth.utils import log as oalog
    from ontquery.utils import log as oqlog
    from augpathlib.utils import log as alog
    from pyontutils.utils import log as pylog

    sfh = SimpleFileHandler(log_file, log)
    sfh(alog, idlog, oalog, oqlog, prlog, pylog)


class _log:
    """ logging prevents nice ipython recurions error printing
        so rename this class to log when you need fake logging """
    @staticmethod
    def debug(nothing): pass
    @staticmethod
    def info(nothing): pass
    @staticmethod
    def warning(nothing): print(nothing)
    @staticmethod
    def error(nothing): pass
    @staticmethod
    def critical(nothing): pass


want_prefixes = ('TEMP', 'FMA', 'UBERON', 'PATO', 'NCBITaxon', 'ilxtr', 'sparc',
                 'BIRNLEX', 'tech', 'unit', 'ILX', 'lex',)


def is_list_or_tuple(obj):
    return isinstance(obj, list) or isinstance(obj, tuple)


def symlink_latest(dump_path, path, relative=True):
    """ relative to allow moves of the containing folder
        without breaking links """

    if relative:
        dump_path = dump_path.relative_path_from(path)

    if path.exists():
        if not path.is_symlink():
            raise TypeError(f'Why is {path.name} not a symlink? '
                            f'{path!r}')

        path.unlink()

    path.symlink_to(dump_path)
