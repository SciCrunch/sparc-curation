import io
import json
import hashlib
import inspect
from pathlib import Path
from functools import wraps
from pyontutils.utils import makeSimpleLogger, python_identifier  # FIXME update imports
from pyontutils.namespaces import sparc
from sparcur.config import config


log = makeSimpleLogger('sparcur')
logd = log.getChild('data')


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


default_cypher = hashlib.blake2b


class etag:

    cypher = hashlib.md5

    def __init__(self, chunksize):
        self.chunksize = chunksize
        self._m = self.cypher()
        self._parts = []
        self._remainder = b''
        self._last_chunksize = 0

    def update(self, bytes_):
        remainder, self._remainder = self._remainder, b''
        bio = io.BytesIO(remainder + bytes_)  # inefficient for len(bytes_) << self.chunksize
        while True:
            if self._last_chunksize:
                # in the event a restart is required only
                # only read the number of bytes left for
                # the current chunk to update self._m
                chunksize = self.chunksize - self._last_chunksize
                self._last_chunksize = 0
            else:
                chunksize = self.chunksize

            chunk = self._remainder
            self._remainder = bio.read(chunksize)
            if not self._remainder:
                # the second time around at the end this will break the loop if the chunk
                # is exactly equal to the chunk size
                if len(chunk) < chunksize:
                    self._remainder = chunk
                    return

            if chunk:
                self._m.update(chunk)  # empty remainder doesn't advance the function
                digest = self._m.digest()
                self._m = self.cypher()
                if chunksize < self.chunksize:
                    # we are in a restart and need to update the last part with the
                    # digest for the full chunksize
                    self._parts[-1] = digest
                else:
                    self._parts.append(digest)

    def digest(self):
        if self._remainder:
            self._last_chunksize = len(self._remainder)  # in case someone calls an early digest
            self._m.update(self._remainder)
            self._parts.append(self._m.digest())  # do not overwrite self._m yet
            self._remainder = b''  # this allows a stable call at the end

        m = self.cypher()
        m.update(b''.join(self._parts))
        return m.digest(), len(self._parts)

    def hexdigest(self):
        digest, count = self.digest()
        return f'{digest.hex()}-{count}'


want_prefixes = ('TEMP', 'FMA', 'UBERON', 'PATO', 'NCBITaxon', 'ilxtr', 'sparc',
                 'BIRNLEX',)


class FileSize(int):
    @property
    def mb(self):
        return self / 1024 ** 2

    @property
    def hr(self):
        """ human readable file size """

        def sizeof_fmt(num, suffix=''):
            for unit in ['','K','M','G','T','P','E','Z']:
                if abs(num) < 1024.0:
                    return "%0.0f%s%s" % (num, unit, suffix)
                num /= 1024.0
            return "%.1f%s%s" % (num, 'Yi', suffix)

        if self is not None and self >= 0:
            return sizeof_fmt(self)
        else:
            return '??'  # sigh

    def __repr__(self):
        return f'{self.__class__.__name__} <{self.hr} {self}>'


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


def cache(folder, ser='json', clear_cache=False, create=False):
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
                    return deserialize(f)
            else:
                output = function(*args, **kwargs)
                if output is not None:
                    with open(filepath, write_mode) as f:
                        serialize(output, f)

                return output

        return superinner

    return inner
