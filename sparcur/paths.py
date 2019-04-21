"""sparcur extensions to pathlib

sparcur makes extensive use of the pathlib Path object (and friends)
by augmenting the base PosixPath object with additional functionality
such as getting and setting xattrs, syncing with other mapped paths etc.

In essence there are 3 ways that a Path object can be used: Local, Cache, and Remote.
Local paths return data and metadata that are local the the current computer.
Cache paths return local metadata about remote objects (such as their remote id).
Remote objects provide an interface to remote data that is associated with a path.

Remote paths should be back by another object which is the representation of the
remote according to the remote's APIs.

Remote paths are only intended to provide a 1:1 mapping, so list(local.data) == list(remote.data)
should always be true if everything is in sync.

If there is additional metadata that is associated with a local path then that is
represented in the layer above this one (currently FThing, in the future a validation Stage).
That said, it does seem like we need a more formal place that can map between all these
things rather than always trying to derive the mappings from data embedded (bound) to
the derefereced path object. """

import sys
import base64
import pickle
import struct
import hashlib
import subprocess
from time import sleep
from pathlib import PosixPath, PurePosixPath
from datetime import datetime
import xattr
import psutil
from dateutil import parser
from Xlib.display import Display
from Xlib import Xatom
from sparcur import exceptions as exc
from sparcur.core import log
from IPython import embed

# remote data about remote objects -> remote_meta
# local data about remote objects -> cache_meta
# local data about local objects -> meta
# remote data about local objects <- not relevant yet? or is this actually rr?

# TODO by convetion we could store 'children' of files in a .filename.ext folder ...
# obviously this doesn't always work
# and we would only want to do this for files that actually had annotations that
# needed a place to live ...

class PathMeta:

    # TODO register xattr prefixes

    @classmethod
    def from_metastore(cls, blob, prefix=None):
        """ db entry """
        xattrs = pickle.loads(blob)
        return cls.from_xattrs(xattrs, prefix)


    def __init__(self,
                 size=None,
                 created=None,
                 updated=None,
                 checksum=None,
                 id=None,
                 file_id=None,
                 old_id=None,
                 gid=None,  # needed to determine local writability
                 user_id=None,
                 mode=None,
                 errors=None,
                 **kwargs):
        self.size = size
        self.created = created
        self.updated = updated
        self.checksum = checksum
        self.id = id
        self.file_id = file_id
        self.old_id = old_id
        self.gid = gid
        self.user_id = user_id
        self.mode = mode
        self.errors = errors
        if kwargs:
            log.warning(f'Unexpected meta values! {kwargs}')
            self.__kwargs = kwargs  # roundtrip values we don't explicitly handle

    #def as_xattrs(self, prefix=None):
        #log.debug(f'{self} {prefix}')
        #embed()
        #return self._as_xattrs(self, prefix=prefix)


    def items(self):
        return self.__dict__.items()  # FIXME nonfields?
        #for field in self.fields:
            #yield field, getattr(self, field)

    @property
    def hrsize(self):
        """ human readable file size """

        def sizeof_fmt(num, suffix=''):
            if num is None:
                raise BaseException(f'wat {dir(self)} {self.size} {self.mode}')
            for unit in ['','K','M','G','T','P','E','Z']:
                if abs(num) < 1024.0:
                    return "%0.0f%s%s" % (num, unit, suffix)
                num /= 1024.0
            return "%.1f%s%s" % (num, 'Yi', suffix)

        if self.size is not None and self.size >= 0:
            return sizeof_fmt(self.size)
        else:
            return '??'  # sigh

    def as_metastore(self, prefix=None):
        # FIXME prefix= is a bad api ...
        """ db entry """  # TODO json blob in sqlite? can it index?
        return pickle.dumps(self.as_xattrs(prefix))

    def __iter__(self):
        yield from (k for k in self.__dict__ if not k.startswith('_'))

    def __repr__(self):
        _dict = {k:v for k, v in self.__dict__.items() if not k.startswith('_')}
        return f'{self.__class__.__name__}({_dict})'

    def __eq__(self, other):
        if isinstance(other, PathMeta):
             for field, value in self.__dict__.items():
                  if value != getattr(other, field):
                       return False
             else:
                  return True

    def __bool__(self):
        for v in self.__dict__.values():
            if v is not None:
                return True
        else:
            return False


class RemotePath:
    """ Remote data about a remote object. """

    _cache_class = None

    # we use a PurePath becuase we still want to key off this being local path
    # but we don't want any of the local file system operations to work by accident
    # so for example self.stat should return the remote value not the local value
    # which is what would happen if we used a PosixPath as the base class

    # need a way to pass the session information in independent of the actual path
    # abstractly having remote.data(global_id_for_local, self)
    # should be more than enough, the path object shouldn't need
    # to know that it has a remote id, the remote manager should
    # know that

    @classmethod
    def setup(cls, local_class, cache_class):
        """ call this once to bind everything together """
        cache_class.setup(local_class, cls)

    def bootstrap(self, recursive=False, only=tuple(), skip=tuple()):
        self.cache.bootstrap(self.meta.id, recursive=recursive, only=only, skip=skip)

    def __init__(self, id, cache=None):
        self.id = id
        self.cache = cache
        self.cache._remote = self

    @property
    def cache(self):
        return self._cache

    @cache.setter
    def cache(self, cache):
        if cache.meta is None:
            cache.meta = self.meta  # also easier than bootstrapping ...

        if cache.id is not None and cache.id != self.id:
            raise exc.MetadataIdMismatchError('Cache id does not match remote id! '
                                              f'{cache.id} != {self.id}\n{cache.meta}')
        if not hasattr(self, '_cache') or self._cache is None:
            self._cache = cache
        else:
            raise BaseException('FIXME make a proper error type, cache exists')

            
    @property
    def local(self):
        return self.cache.local

    @property
    def anchor(self):
        raise NotImplementedError

    @property
    def id(self):
        raise NotImplementedError
        self.remote_thing.id_from_machine_id_and_path(self)

    @property
    def data(self):
        self.cache.id
        for chunk in chunks:
            yield chunk

    @property
    def meta(self):
        # on blackfynn this is the package id or object id
        # this will error if there is no implementaiton if self.id
        return PathMeta(id=self.id)

    @meta.setter
    def meta(self, value):
        raise NotImplementedError
        return 'yes?'

    @property
    def annotations(self):
        # these are models etc in blackfynn
        yield from []
        raise NotImplementedError

    @property
    def children(self):
        # uniform interface for retrieving remote hierarchies decoupled from meta
        raise NotImplementedError

    @property
    def rchildren(self):
        # uniform interface for retrieving remote hierarchies decoupled from meta
        raise NotImplementedError

    def iterdir(self):
        # I'm guessing most remotes don't support this
        raise NotImplementedError

    def glob(self, pattern):
        raise NotImplementedError

    def rglob(self, pattern):
        raise NotImplementedError


class CachePath(PosixPath):  # this needs to be a real path so that it can navigate the local path sturcture
    """ Local data about remote objects.
        This is where the mapping between the local id (aka path)
        and the remote id lives. In a git-like world this is the
        cache/index/whatever we call it these days 
    
        This is the bridge class that holds the mappings.
        Always start bootstrapping from one of these classes
        since it has both the local and remote identifiers,
        and therefore can be called and used before specifying
        the exact implementations for the local and remote objects.
    """

    _local_class = None
    _remote_class_factory = None

    @classmethod
    def setup(cls, local_class, remote_class_factory):
        """ call this once to bind everything together """
        cls._local_class = local_class
        cls._remote_class_factory = remote_class_factory
        local_class._cache_class = cls
        remote_class_factory._cache_class = cls

    def bootstrap(self, id, *, parents=False, recursive=False,
                  fetch_data=False, size_limit_mb=2,
                  only=tuple(), skip=tuple()):
        try:
            self._in_bootstrap = True
            self._bootstrap(id, parents=parents,
                            recursive=recursive,
                            fetch_data=fetch_data,
                            size_limit_mb=size_limit_mb,
                            only=only, skip=skip)
        finally:
            delattr(self, '_in_bootstrap')
            if hasattr(self, '_meta'):
                delattr(self, '_meta')

    def _bootstrap(self, id, *, parents=False, recursive=False,
                   fetch_data=False, size_limit_mb=2,
                   only=tuple(), skip=tuple()):
        """ The actual bootstrap implementation """

        if only or skip:
            if id.startswith('N:organization:'):  # FIXME :/
                # since we only go one organization at a time right now
                # we never want to skip the top level id
                log.info(f'Bootstrapping {id}')
            elif id in skip:
                log.info(f'Skipped       {id} since it is in skip')
                return
            elif only and id not in only:
                log.info(f'Skipped       {id} since it is not in only')
                return
            else:
                # if you pass the only mask so do your children
                log.info(f'Bootstrapping {id}')
                only = tuple()

        if not self.meta:
            meta = PathMeta(id=id)

        elif self.meta.id is None:
            log.warning(f'Existing meta for {self} no id so overwriting\n{self.meta}')
            meta = PathMeta(id=id)

        elif self.id != id:
            # TODO overwrite
            raise exc.MetadataIdMismatchError('Existing cache id does not match new id! '
                                              f'{self.meta.id} != {id}\n{self.meta}')
        else:
            meta = self.meta  # HOW DID THIS GET SET AND BY WHO WHEN?!?  FIXME FIXME

        if not self.is_symlink() and self.exists():
            if self.meta and self.meta.id:
                # we have our files, update the metdata while we're at it
                self.meta = self.remote.meta

        elif not self.is_symlink() and (not self.exists() or
                                        self.exists() and
                                        not self.meta and
                                        not list(self.local.children)):
            if not self.meta:
                log.debug('problem')
                self.meta = meta  # memory only bootstrap

            remote_meta = self.remote.meta
            if self.remote.is_dir():
                self.mkdir(parents=parents)
                if not self.meta:
                    self.meta = meta  # save bootstrap meta to disk
                self.meta = remote_meta

            # FIXME this is really select remote cache type?
            elif self.remote.is_file():
                if not self.parent.exists():
                    self.parent.mkdir(parents=parents)

                if fetch_data and self.meta.size < size_limit_mb * 1024 ** 2:
                    # running this first means that we will use xattrs instead of symlinks
                    # this is a bit opaque, but since meta uses a setter we can't pass a
                    # param to make it clear (oh look, python being dumb again!)
                    self.touch()

                if not self.meta:
                    self.meta = meta  # save bootstrap meta to disk as xattr or symlink

                self.meta = remote_meta
                if self.meta != remote_meta:
                    # read back from storage to be sure ... annnd we failed
                    msg = '\n'.join([f'{k!r} {v!r} {getattr(remote_meta, k)!r}'
                                     for k, v in self.meta.items()])
                    raise exc.MetadataCorruptionError(msg)

                if fetch_data and self.meta.size < size_limit_mb * 1024 ** 2:
                    self.local.data = self.remote.data

                if self.meta.checksum:
                    lc = self.local.meta.checksum 
                    cc = self.meta.checksum
                    if lc != cc:
                        raise exc.ChecksumError('Checksums to not match! {lc} != {cc}')
                else:
                    log.warning(f'No checksum for {self}! Your data is at risk!')

            else:
                raise BaseException(f'Remote is not a file or directory {self}')

        if recursive:
            if id.startswith('N:organization:'):  # FIXME :/
                for child in self.remote.children:
                    child.bootstrap(recursive=True, only=only, skip=skip)
            else:
                for child in self.remote.rchildren:
                    # use the remote's recursive implementation
                    # not the local implementation, since the
                    # remote may have additional requirements
                    child.bootstrap(only=only, skip=skip)

    @property
    def remote(self):
        if self._remote_class_factory is not None:
            # we don't have to have a remote configured to check the cache
            if not hasattr(self, '_remote_class'):
                log.debug('rc')
                # NOTE there are many possible ways to set the anchor
                # we need to pick _one_ of them
                self._remote_class = self._remote_class_factory(self.anchor,
                                                                self._local_class,
                                                                self.__class__)

            if not hasattr(self, '_remote'):
                #log.debug('r')
                self.remote = self._remote_class(self.id, self)

            return self._remote

    @remote.setter
    def remote(self, remote):
        if self.meta is not None and self.id is not None and hasattr(self, '_remote'):
            if self.id != self.remote.id:
                raise exc.MetadataIdMismatchError('Existing cache id does not match new id! '
                                                  f'{self.meta.id} != {id}\n{self.meta}')

            elif isinstance(remote, RemotePath):
                self._remote = remote
                remote._cache = self
            else:
                raise TypeError(f'{remote} is not a RemotePath! It is a {type(remote)}')

        else:
            # make sure no monkey business is going on at least in the local graph
            if self.parent and self.local.parent.cache.meta.id == self.parent.id:
                # wow is that a vastly easier bootstrap ...
                self.meta = remote.meta
                self._remote = remote
                remote._cache = self

    @property
    def local(self):
        if self._local_class is not None:
            if not hasattr(self, '_local'):
                self._local = self._local_class(self)
                self._local._cache = self

            return self._local

    @property
    def id(self):
        return self.meta.id

    # TODO how to toggle fetch from remote to heal?
    @property
    def meta(self):
        if hasattr(self, '_meta'):
            return self._meta  # for bootstrap

    @meta.setter
    def meta(self, pathmeta):
        raise NotImplementedError

    def _meta_setter(self, pathmeta):
        """ so much for the pythonic way when the language won't even let you """
        #if hasattr(self, '_in_bootstrap'):
        #if not hasattr(self, '_meta'):
        #log.warning(f'!!!!!!!!!!!!!!!!!!!!!!!!1 {self}')
        if self.meta and self.id != pathmeta.id:
            raise exc.MetadataIdMismatchError('Cache id does not match meta id! '
                                              f'{self.id} != {pathmeta.id}\n{pathmeta}')

        self._meta = pathmeta
        #else:
            # if you get here you probably forgot to implement @meta.setter for your cache class
            #msg = 'Trying to set memory only metadata while not in bootstrap.'
            #log.error(msg)
            #raise exc.NotBootstrappingError(msg)

    @property
    def data(self):
        # we don't keep two copies of the local data
        # unless we are doing a git-like thing
        raise NotImplementedError

    def __repr__(self):
        local = repr(self.local) if self.local else str(self)
        remote = (f'{self.remote.__class__.__name__}({self.remote.meta.id!r})'
                  if self.remote else self.id)
        return local + ' -> ' + remote



class _PathMetaAsSymlink:
    empty = '#'
    fieldsep = '.'  # must match the file extension splitter for Path ...
    subfieldsep = ';'  # only one level, not going recursive in a filename ...
    order = ('size',
             'created',
             'updated',
             'checksum',
             'old_id',
             'gid',
             'user_id',
             'mode',
             'errors',)
    extras = 'hrsize',
    order_all = order + extras
    pathmetaclass = PathMeta

    def __init__(self):
        # register functionality on PathMeta
        def as_symlink(self, _as_symlink=self.as_symlink):
            return _as_symlink(self)

        @classmethod
        def from_symlink(cls, symlink_path, _from_symlink=self.from_symlink):
            return _from_symlink(symlink_path)

        self.pathmetaclass.as_symlink = as_symlink
        self.pathmetaclass.from_symlink = from_symlink

    def encode(self, field, value):
        if value is None:
            return self.empty

        if field in ('errors',):
            return self.subfieldsep.join(value)

        value = str(value)
        value = value.replace(self.fieldsep, ',')
        return value

    def decode(self, field, value):
        value = value.strip(self.fieldsep)

        if value == self.empty:
            return None

        if field == 'errors':
            return [_ for _ in value.split(self.subfieldsep) if _]

        elif field in ('created', 'updated'):
            return parser.parse(value)

        elif field == 'checksum':  # FIXME checksum encoding ...
            #return value.encode()
            return value.encode()

        elif field == 'user_id':
            try:
                return int(value)
            except ValueError:  # FIXME :/ uid vs owner_id etc ...
                return value.decode()

        elif field in ('id', 'mode'):
            return value

        else:
            return int(value)

        return value

    def as_symlink(self, pathmeta):
        """ encode meta as a relative path """

        gen = (self.encode(field, getattr(pathmeta, field))
               for field in self.order_all)

        return PosixPath(pathmeta.id + '/.meta.' + self.fieldsep.join(gen))

    def from_symlink(self, symlink_path):
        parent = symlink_path.parent
        relative_path = symlink_path.resolve().relative_to(parent.resolve())
        # this only deals with the relative path
        kwargs = {field:self.decode(field, value)
                  for field, value in zip(self.order, relative_path.suffixes)}
        kwargs['id'] = str(parent)
        return self.pathmetaclass(**kwargs)

    #@classmethod
    #def from_symlink(cls, symlink_path):
        #if not symlink_path.is_symlink():
            #raise TypeError(f'Not a symlink! {symlink_path}')
        #return cls.symlink.from_symlink(symlink_path)


class SymlinkCache(CachePath):

    pathmeta = _PathMetaAsSymlink()

    @property
    def meta(self):
        # FIXME symlink cache!??!
        if self.is_symlink():
            if not self.exists():  # if a symlink exists it is something else
                return pathmeta.from_symlink(self)
            else:
                raise exc.PathExistsError(f'Target of symlink exists!\n{self} -> {self.resolve()}')

        else:
            return super().meta

    @meta.setter
    def meta(self, pathmeta):
        if not self.exists():
            # if the path does not exist write even temporary to disk
            if self.is_symlink():
                if self.meta.id != pathmeta.id:
                    raise exc.MetadataIdMismatchError('Existing cache id does not match new id! '
                                                      f'{self.meta.id} != {id}\n{self.meta}')

                log.debug('existing metadata found, but ids match so will update')

            self.local.symlink_to(pathmeta.as_symlink(pathmeta))

        else:
            raise exc.PathExistsError(f'Path exists {self}')


class _PathMetaAsXattrs:
    fields = ('size', 'created', 'updated',
              'checksum', 'id', 'file_id', 'old_id',
              'gid', 'user_id', 'mode', 'errors')

    encoding = 'utf-8'

    pathmetaclass = PathMeta

    def __init__(self):
        # register functionality on PathMeta
        def as_xattrs(self, prefix=None, _as_xattrs=self.as_xattrs):
            #log.debug(f'{self} {prefix}')
            return _as_xattrs(self, prefix=prefix)

        @classmethod
        def from_xattrs(cls, xattrs, prefix=None, path_object=None, _from_xattrs=self.from_xattrs):
            # FIXME cls(**kwargs) vs calling self.pathmetaclass
            return _from_xattrs(xattrs, prefix=prefix, path_object=path_object)

        self.pathmetaclass.as_xattrs = as_xattrs
        self.pathmetaclass.from_xattrs = from_xattrs

    @staticmethod
    def deprefix(string, prefix):
        if string.startswith(prefix):
            string = string[len(prefix):]

        # deal with normalizing old form here until it is all cleaned up
        if prefix == 'bf.':
            if string.endswith('_at'):
                string = string[:-3]

        return string

    def from_xattrs(self, xattrs, prefix=None, path_object=None):
        """ decoding from bytes """
        _decode = getattr(path_object, 'decode_value', None)
        if path_object and _decode is not None:
            # some classes may need their own encoding rules _FOR NOW_
            # we will remove them once we standardize the xattrs format
            def decode(field, value, dv=_decode):
                out = dv(field, value)
                if value is not None and out is None:
                    out = self.decode(field, value)

                return out
                
        else:
            decode = self.decode


        if prefix:
            prefix += '.'
            kwargs = {k:decode(k, v)
                      for kraw, v in xattrs.items()
                      for k in (self.deprefix(kraw.decode(self.encoding), prefix),)}
        else:  # ah manual optimization
            kwargs = {k:decode(k, v)
                      for kraw, v in xattrs.items()
                      for k in (kraw.decode(self.encoding),)}

        return self.pathmetaclass(**kwargs)

    def as_xattrs(self, pathmeta, prefix=None):
        """ encoding to bytes """
        #log.debug(pathmeta)
        out = {}
        for field in self.fields:
            value = getattr(pathmeta, field)
            if value:
                value_bytes = self.encode(field, value)
                if prefix:
                    key = prefix + '.' + field
                else:
                    key = field

                key_bytes = key.encode(self.encoding)
                out[key_bytes] = self.encode(field, value)

        return out

    def encode(self, field, value):
        #if field in ('created', 'updated') and not isinstance(value, datetime):
            #field.replace(cls.path_field_sep, ',')  # FIXME hack around iso8601
            # turns it 8601 isnt actually standard >_< with . instead of , sigh

        if field == 'errors':
            value = ';'.join(value)
            
        if isinstance(value, datetime):  # FIXME :/ vs iso8601
            #value = value.isoformat().isoformat().replace('.', ',')
            value = value.timestamp()  # I hate dealing with time :/

        if isinstance(value, int):
            # this is local and will pass through here before move?
            #out = value.to_bytes(value.bit_length() , sys.byteorder)
            out = str(value).encode(self.encoding)  # better to have human readable
        elif isinstance(value, float):
            out = struct.pack('d', value) #= bytes(value.hex())
        elif isinstance(value, str):
            out = value.encode(self.encoding)
        else:
            raise exc.UnhandledTypeError(f'dont know what to do with {value!r}')

        return out

    def decode(self, field, value):
        if field in ('created', 'updated'):  # FIXME human readable vs integer :/
            value, = struct.unpack('d', value)
            return datetime.fromtimestamp(value)
        elif field == 'checksum':
            return value
        elif field == 'errors':
            value = value.decode(self.encoding)
            return tuple(_ for _ in value.split(';') if _)
        elif field == 'user_id':
            try:
                return int(value)
            except ValueError:  # FIXME :/ uid vs owner_id etc ...
                return value.decode()
        elif field in ('id', 'mode'):
            return value.decode(self.encoding)
        elif field not in self.fields:
            log.warning(f'Unhandled field {field}')
            return value
        else:
            try:
                return int(value)
            except ValueError as e:
                print(field, value)
                raise e


class XattrPath(PosixPath):
    """ pathlib Path augmented with xattr support """

    def setxattr(self, key, value, namespace=xattr.NS_USER):
        if not isinstance(value, bytes):  # checksums
            bytes_value = str(value).encode()  # too smart? force pre encoded?
        else:
            bytes_value = value

        xattr.set(self.as_posix(), key, bytes_value, namespace=namespace)

    def setxattrs(self, xattr_dict, namespace=xattr.NS_USER):
        for k, v in xattr_dict.items():
            self.setxattr(k, v, namespace=namespace)

    def getxattr(self, key, namespace=xattr.NS_USER):
        # we don't deal with types here, we just act as a dumb store
        return xattr.get(self.as_posix(), key, namespace=namespace)

    def xattrs(self, namespace=xattr.NS_USER):
        # decode keys later
        try:
            return {k:v for k, v in xattr.get_all(self.as_posix(), namespace=namespace)}
        except FileNotFoundError as e:
            raise FileNotFoundError(self) from e


class XattrCache(CachePath, XattrPath):
    xattr_prefix = None
    pathmeta = _PathMetaAsXattrs()

    @property
    def meta(self):
        # FIXME symlink cache!??!
        if self.exists():
            xattrs = self.xattrs()
            pathmeta = self.pathmeta.from_xattrs(self.xattrs(), self.xattr_prefix, self)
            return pathmeta
        else:
            return super().meta

    @meta.setter
    def meta(self, pathmeta):
        #log.warning(f'!!!!!!!!!!!!!!!!!!!!!!!!2 {self}')
        # TODO cooperatively setting multiple different cache types?
        # do we need to use super() or something?

        if self.exists():
            if self.is_symlink():
                raise TypeError('will not write meta on symlinks! {self}')
            self.setxattrs(pathmeta.as_xattrs(self.xattr_prefix))
            if hasattr(self, '_meta'):  # prevent reading from in-memory store
                delattr(self, '_meta')

        else:
            # the glories of the inconsistencies and irreglarities of python
            # you can't setattr using super() so yes you _do_ actually have to
            # implement a setter sometimes >_<
            super()._meta_setter(pathmeta)


class SqliteCache(CachePath):
    """ a persistent store to back up the xattrs if they get wiped """


class BlackfynnCache(SqliteCache, XattrCache):
    xattr_prefix = 'bf'

    @classmethod
    def decode_value(cls, field, value):
        if field in ('created', 'updated'):
            return parser.parse(value.decode())  # FIXME with timezone vs without ...

    @property
    def anchor(self):
        return self.organization

    @property
    def organization(self):
        """ organization represents a permissioning boundary
            for blackfynn, so above this we would have to know
            in advance the id and have api keys for it and the
            containing folder would have some non-blackfynn notes
            also it seems likely that the same data could appear in
            multiple different orgs, so that would mean linking locally
        """

        # FIXME in reality files can have more than one org ...
        if self.meta is not None:
            id = self.id
            if id is None:
                parent = self.parent
                if parent == self:  # we have hit a root
                    return None

                organization = parent.organization

                if organization is not None:
                    # TODO repair
                    pass
                else:
                    raise exc.NotInProjectError()

            elif id.startswith('N:organization:'):
                return self

        if self.parent:
            # we have a case of missing metadata here as well
            return self.parent.organization

    @property
    def dataset(self):
        if self.meta.id.startswith('N:dataset:'):
            return self
        elif self.parent:
            return self.parent.dataset

    @property
    def human_uri(self):
        # org /datasets/ N:dataset /files/ N:collection
        # org /datasets/ N:dataset /files/ wat? /N:package  # opaque but consistent id??
        # org /datasets/ N:dataset /viewer/ N:package
        id = self.id
        N, type, suffix = id.split(':')
        if id.startswith('N:package:'):
            prefix = '/viewer/'
        elif id.startswith('N:collection:'):
            prefix = '/files/'
        elif id.startswith('N:dataset:'):
            prefix = '/datasets/'
            return self.parent.human_uri + prefix + id
        elif id.startswith('N:organization:'):
            return 'https://app.blackfynn.io/' + id
        else:
            raise exc.UnhandledTypeError(type)

        return self.dataset.human_uri + prefix + id

    @property
    def children(self):
        """ direct children """
        # if you want the local children go on local
        # this will give us the remote children in the local context
        # going in the reverse direction with parents
        # we don't do because the parents here are already defined
        # if a file has moved on the remote we can detect that and error for now
        for child in self.remote.children:
            child_path = self / child.name
            child_path.remote = child

            yield child_path

    @property
    def rchildren(self):
        # have to express the generator to build the index
        # otherwise child.parents will not work correctly (annoying)
        for child in self.remote.rchildren:
            parent_names = []  # FIXME massive inefficient due to retreading subpaths :/
            for parent in child.parents:
                if parent == self.remote:
                    break
                else:
                    parent_names.append(parent.name)

            args = (*reversed(parent_names), child.name)
            #log.debug(' '.join((str(_) for _ in (self.__class__, self, args))))
            child_path = self.__class__(self, *args)
            child_path.remote = child

            yield child_path

        # if organization
        # if dataset (usually going to be the fastest in most cases)
        # if collection (can end up very slow)
        # if package/file


class LocalPath(PosixPath):
    # local data about remote objects

    _cache_class = None  # must be defined by child classes
    sysuid = None  # set below

    @classmethod
    def setup(cls, cache_class, remote_class_factory):
        """ call this once to bind everything together """
        cache_class.setup(cls, remote_class_factory )

    @property
    def remote(self):
        return self.cache.remote

    @remote.setter
    def remote(self, remote):
        self.cache.remote = remote

    @property
    def cache(self):
        # TODO performance check on these
        if not hasattr(self, '_cache'):
            self._cache = self._cache_class(self)
            self._cache._local = self

        return self._cache

    #@property
    #def id(self):  # FIXME reuse of the name here could be confusing, though it is technically correct
        #""" THERE CAN BE ONLY ONE """
        #return self.resolve().as_posix()

    @property
    def created(self):
        self.meta.created

    @property
    def meta(self):
        st = self.stat()
        updated = datetime.fromtimestamp(st.st_mtime)  #.isoformat().replace('.', ',')  # TODO
        # these use our internal representation of timestamps
        # the choice of how to store them in xattrs, sqlite, json, etc is handled at those interfaces
        # replace with comma since it is conformant to the standard _and_
        # because it simplifies PathMeta as_path
        mode = oct(st.st_mode)
        return PathMeta(size=st.st_size,
                        created=None,
                        updated=updated,
                        checksum=self.checksum(),
                        id=self.sysuid + ':' + self.as_posix(),
                        user_id=st.st_uid,  # keep in mind that a @meta.setter will require a coverter
                        gid=st.st_gid,
                        mode=mode)

    @property
    def data(self):
        with open(self, 'rb') as f:
            while True:
                data = f.read(4096)  # TODO hinting
                if not data:
                    break

                yield data

    @data.setter
    def data(self, generator):
        meta = self.meta
        log.debug(f'writing to {self}')
        with open(self, 'wb') as f:
            for chunk in generator:
                log.debug(chunk)
                f.write(chunk)

        self.meta = meta  # glories of persisting xattrs :/

    @property
    def children(self):
        yield from self.iterdir()

    @property
    def rchildren(self):
        yield from self.rglob('*')

    def diff(self):
        pass

    def meta_to_remote(self):
        # pretty sure that we don't wan't this independent of data_to_remote
        # sort of cp vs cp -a and commit date vs author date
        raise NotImplementedError
        meta = self.meta
        # FIXME how do we invalidate cache?
        self.remote.meta = meta  # this can super duper fail

    def data_to_remote(self):
        raise NotImplementedError
        self.remote.data = self.data

    def annotations_to_remote(self):
        raise NotImplementedError
        self.remote.data = self.data

    def to_remote(self):  # push could work ...
        # FIXME in theory we could have an endpoint for each of these
        # The remote will handle that?
        # this can definitely fail
        raise NotImplementedError('need the think about how to do this without causing disasters')
        self.remote.meta = self.meta
        self.remote.data = self.data
        self.remote.annotations = self.annotations

    def checksum(self, cypher=hashlib.sha256):
        if self.is_file():
            m = cypher()
            chunk_size = 4096
            with open(self, 'rb') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if chunk:
                        m.update(chunk)
                    else:
                        break

            return m.digest()


LocalPath.sysuid = base64.urlsafe_b64encode(LocalPath('/etc/machine-id').checksum()[:16])[:-2].decode()


class Path(LocalPath):  # NOTE this is a hack to keep everything consisten
    """ An augmented path for all the various local needs of the curation process. """
    _cache_class = BlackfynnCache

    def xopen(self):
        """ open file using xdg-open """
        process = subprocess.Popen(['xdg-open', self.as_posix()],
                                   stdout=subprocess.DEVNULL,
                                   stderr=subprocess.STDOUT)

        return  # FIXME this doesn't seem to update anything beyond python??
        pid = process.pid
        proc = psutil.Process(pid)
        process_window = None
        while not process_window:  # FIXME ick
            sprocs = [proc] + [p for p in proc.children(recursive=True)]
            if len(sprocs) < 2:  # xdg-open needs to call at least one more thing
                sleep(.01)  # spin a bit more slowly
                continue

            wpids = [s.pid for s in sprocs][::-1]  # start deepest work up
            # FIXME expensive to create this every time ...
            disp = Display()
            root = disp.screen().root
            children = root.query_tree().children
            #names = [c.get_wm_name() for c in children if hasattr(c, 'get_wm_name')]
            try:
                by_pid = {c.get_full_property(disp.intern_atom('_NET_WM_PID'), 0):c for c in children}
            except Xlib.error.BadWindow:
                sleep(.01)  # spin a bit more slowly
                continue

            process_to_window = {p.value[0]:c for p, c in by_pid.items() if p}
            for wp in wpids:
                if wp in process_to_window:
                    process_window = process_to_window[wp]
                    break

            if process_window:
                name = process_window.get_wm_name()
                new_name = name + ' ' + self.resolve().as_posix()[-30:]
                break  # TODO search by pid is broken, but if you can find it it will work ...
                # https://github.com/jordansissel/xdotool/issues/14 some crazy bugs there
                command = ['xdotool', 'search','--pid', str(wp), 'set_window', '--name', f'"{new_name}"']
                subprocess.Popen(command,
                                 stdout=subprocess.DEVNULL,
                                 stderr=subprocess.STDOUT)
                print(' '.join(command))
                break
                process_window.set_wm_name(new_name)
                embed()
                break
            else:
                sleep(.01)  # spin a bit more slowly
