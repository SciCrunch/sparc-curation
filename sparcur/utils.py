import io
import json
import hashlib
import inspect
import logging
from pathlib import Path
from functools import wraps
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


_type_order = (
    bool, int, float, bytes, str, tuple, list, set, dict, object, type, None
)


def type_index(v):
    for i, _type in enumerate(_type_order):
        if isinstance(v, _type):
            return i

    return -1


def args_sort_key(kv):
    """ type ordering """
    k, v = kv
    return k, type_index(v), v


def cache_hash(pairs, cypher=hashlib.blake2s):
    pairs = sorted(pairs, key=args_sort_key)
    converted = []
    for k, v in pairs:
        if k == 'self':  # FIXME convention only ...
            v, _v = v.__class__, v
        converted.append(k.encode() + b'\x01' + str(v).encode())

    message = b'\x02'.join(converted)
    m = cypher()
    m.update(message)
    return m.hexdigest()


def argspector(function):
    argspec = inspect.getfullargspec(function)
    def spector(*args, **kwargs):
        for i, (k, v) in enumerate(zip(argspec.args, args)):
            yield k, v

        if argspec.varargs is not None:
            for v in args[i:]:
                yield argspec.varargs, v

        for k, v in kwargs.items():
            yield k, v

    return spector


def cache(folder, ser='json', clear_cache=False, create=False, return_path=False):
    """ outer decorator to cache output of a function to a folder """
        

    if ser == 'json':
        serialize = json.dump
        deserialize = json.load
        mode = 't'
    else:
        raise TypeError('Bad serialization format.')

    write_mode = 'w' + mode
    read_mode = 'r' + mode

    folder = Path(folder)
    if not folder.exists():
        if not create:
            raise FileNotFoundError(f'Cache base folder does not exist! {folder}')

        folder.mkdir(parents=True)

    if clear_cache:
        log.debug(f'clearing cache for {folder}')
        shutil.rmtree(folder)
        folder.mkdir()

    def inner(function):
        spector = argspector(function)
        fn = function.__name__
        @wraps(function)
        def superinner(*args, **kwargs):
            filename = cache_hash(spector(*args, ____fn=fn, **kwargs))
            filepath = folder / filename
            if filepath.exists():
                log.debug(f'deserializing from {filepath}')
                with open(filepath, read_mode) as f:
                    output = deserialize(f)
            else:
                output = function(*args, **kwargs)
                if output is not None:
                    with open(filepath, write_mode) as f:
                        serialize(output, f)

            if return_path:
                return output, filepath
            else:
                return output

        return superinner

    return inner


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
