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
    empty = '#'
    path_order = ('file_id',
                  'size',
                  'created',
                  'updated',
                  'checksum',
                  'old_id',
                  'error',
                  'hrsize')

    fields = ('size', 'created', 'updated',
              'checksum', 'id', 'file_id', 'old_id',
              'gid', 'uid', 'mode', 'error')

    encoding = 'utf-8'

    # TODO register xattr prefixes

    @staticmethod
    def deprefix(string, prefix):
        if string.startswith(prefix):
            string = string[len(prefix):]

        # deal with normalizing old form here until it is all cleaned up
        if prefix == 'bf.':
            if string.endswith('_at'):
                string = string[:-3]

        return string

    @classmethod
    def from_xattrs(cls, xattrs, prefix=None, path_object=None):
        """ decoding from bytes """
        if path_object:
            # some classes may need their own encoding rules _FOR NOW_
            # we will remove them once we standardize the xattrs format
            decode_value = getattr(path_object, 'decode_value', cls.decode_value)
        else:
            decode_value = cls.decode_value

        if prefix:
            prefix += '.'
            kwargs = {k:decode_value(k, v)
                      for kraw, v in xattrs.items()
                      for k in (cls.deprefix(kraw.decode(cls.encoding), prefix),)}
        else:  # ah manual optimization
            kwargs = {k:decode_value(k, v)
                      for kraw, v in xattrs.items()
                      for k in (kraw.decode(cls.encoding),)}

        return cls(**kwargs)

    @classmethod
    def from_metastore(cls, blob, prefix=None):
        """ db entry """
        xattrs = pickle.loads(blob)
        return cls.from_xattrs(xattrs, prefix)

    @classmethod
    def from_path(cls, relative_path):
        # this only deals with the relative path
        kwargs = {k:s.strip('.') for k, s in zip(cls.path_order, relative_path.suffixes)}
        return cls(**kwargs)

    def __init__(self,
                 size=None,
                 created=None,
                 updated=None,
                 checksum=None,
                 id=None,
                 file_id=None,
                 old_id=None,
                 gid=None,  # needed to determine local writability
                 uid=None,
                 mode=None,
                 error=None):
        self.size = size
        self.created = created
        self.updated = updated
        self.checksum = checksum
        self.id = id
        self.file_id = file_id
        self.old_id = old_id
        self.gid = gid
        self.uid = uid
        self.mode = mode
        self.error= error

    def items(self):
        for field in self.fields:
            yield field, getattr(self, field)

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

        return sizeof_fmt(self.size)

    @classmethod
    def decode_value(cls, field, value):
        if field in ('created', 'updated'):
            value, = struct.unpack('d', value)
            return datetime.fromtimestamp(value)
        elif field == 'checksum':
            return value
        elif field in ('error', 'id', 'mode'):
            return value.decode(cls.encoding)
        else:
            return int(value)

    @classmethod
    def encode_value(cls, field, value):
        if isinstance(value, datetime):
            #value = value.isoformat().isoformat().replace('.', ',')
            value = value.timestamp()  # I hate dealing with time :/

        if isinstance(value, int):
            # this is local and will pass through here before move?
            #out = value.to_bytes(value.bit_length() , sys.byteorder)
            out = str(value).encode(cls.encoding)  # better to have human readable
        elif isinstance(value, float):
            out = struct.pack('d', value) #= bytes(value.hex())
        elif isinstance(value, str):
            out = value.encode(cls.encoding)
        else:
            raise exc.UnhandledTypeError(f'dont know what to do with {value!r}')

        return out

    def as_xattrs(self, prefix=None):
        """ encoding to bytes """
        out = {}
        for field in self.fields:
            value = getattr(self, field)
            if value:
                value_bytes = self.encode_value(field, value)
                if prefix:
                    key = prefix + '.' + field
                else:
                    key = field

                key_bytes = key.encode(self.encoding)
                out[key_bytes] = self.encode_value(field, value)

        return out

    def as_metastore(self, prefix=None):
        # FIXME prefix= is a bad api ...
        """ db entry """  # TODO json blob in sqlite? can it index?
        return pickle.dumps(self.as_xattrs(prefix))

    def as_path(self):
        """ encode meta as a relative path """
        gen = (str(_) if _ else self.empty
               for _ in (getattr(self, o) for o in self.path_order))
        #return '/'.join(gen)
        return self.id + '/.' + '.'.join(gen)

    def __repr__(self):
        return f'{self.__class__.__name__}({self.__dict__})'

    def __eq__(self, other):
        if isinstance(other, PathMeta):
             for field in self.fields:
                  if getattr(self, field) != getattr(other, field):
                       return False
             else:
                  return True

    def __neg__(self):
        return not any(getattr(self, field) for field in self.fields)


class RemotePath(PurePosixPath):
    """ Remote data about a local object. """

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

    def bootstrap(self, recursive=False):
        self.cache.bootstrap(self.meta.id, recursive=recursive)

    def __new__(cls, *args, **kwargs):
        return super().__new__(cls, *args, **kwargs)

    def __init__(self, *args, **kwargs):
        super().__init__()

    @property
    def cache(self):
        # TODO performance check on these
        if not hasattr(self, '_cache'):
            self._cache = self._cache_class(self)
            self._cache._remote = self

        return self._cache

    @property
    def local(self):
        return self.cache.local

    @property
    def anchor(self):
        raise NotImplementedError

    @property
    def id(self):
        # if you remote supports this query then there is a chance we
        # can pull this off otherwise we have to go via cache
        # essentially the remote endpoint has to know based on
        # _something_ how to construct its version of the local
        # identifier, this will require some additional information

        # assume that there are only 3 things
        # users (uniquely identified remotely authed)
        # root file systems (are not 1:1 with machines)
        # paths (files/folders)

        # we need to add one more, which is the data
        # located at a path, which can change

        # then to construct the inverse mapping we actually only need
        # to identify the file system and the path or paths on that
        # file sytem that are all equivalent resolve() helps with
        # this, not sure about hardlinks, which are evil

        # multiple users can have the 'same' file but if a user
        # doesn't have write access to a file on a file system then we
        # can't put it back for them this happens frequently when
        # people have the same username on their own systems but
        # different usernames on a shared system

        # because kernels (of all kinds) are the principle machine
        # agents that we have to deal with here (including chrooted
        # agents, jails, vms etc.)  we deal with each of them as if
        # they are seeing different data, we probably do want to try
        # to obtain a mapping e.g. via fstab so let's assume ipv6
        # address of the root?  no? how can we tell who is answering?

        # answer ssh host keys? that seems good enough for me, yes
        # maybe people will change host keys, but you can't have more
        # than one at the same time, and you can probably try to
        # bridge a change like that if the hostname stays the same and
        # the user stays the same, or even simpler, if the files that
        # we care about stay the same AND the old/other host cannot be
        # contacted, more like, we are on the host if someone is crazy
        # enough to reuse host keys well ...  wow, apparently this
        # happens quite frequently with vms *headdesk* this requires
        # a real threat model, which we are just going to say is out
        # of scope at the moment, /etc/machine-id is another option
        # but has the same problem as the ssh host key ...

        # windows
        # HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Cryptography
        # (Get-CimInstance -Class Win32_ComputerSystemProduct).UUID

        # answer inside of a vcs: use the identifier of the first
        # commit and the last known good commit ... or similar

        #self.querything.id_from_local(self.local.id)
        #self.remote_thing.id_from_ssh_host_key_and_path(self)

        # remote_thing can get itself the machine id hash plus a constant
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

    def iterdir(self):
        # I'm guessing most remotes don't support this
        raise NotImplementedError

    def glob(self, pattern):
        raise NotImplementedError

    def rglob(self, pattern):
        raise NotImplementedError


class CachePath(PosixPath):
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

    def bootstrap(self, id, *, parents=False, recursive=False):
        #if id is None and self.cache.id is not None:
            #id = self.cache.id
        #else:
            #raise TypeError(f'No cache so id is a required argument')

        if not self.meta:
            meta = PathMeta(id=id)

        elif self.id != id:
            # TODO overwrite
            raise exc.MetadataIdMismatchError(f'Existing cache id does not match new id! {meta.id} != {id}')

        if not self.exists():
            if self.is_dir():
                self.mkdir(parents=parents)
                self.meta = meta  # the bootstrap
                self.meta = self.remote.meta

            elif self.is_file():
                if not self.parent.exists():
                    self.parent.mkdir(parents=parents)

                if fetch_data:
                    self.touch()  # do this so we can write our meta before data
                    self.meta = meta  # the bootstrap
                    self.meta = self.remote.meta
                    self.local.data = self.remote.data

                else:
                    self.local.symlink_to(self.remote.meta.as_path())

        if recursive:
            for child in self.remote.rchildren:
                # use the remote's recursive implementation
                # not the local implementation, since the
                # remote may have additional requirements
                child.bootstrap()

    @property
    def remote(self):
        if self._remote_class_factory is not None:
            # we don't have to have a remote configured to check the cache
            if not hasattr(self, '_remote_class'):
                # NOTE there are many possible ways to set the anchor
                # we need to pick _one_ of them
                self._remote_class = self._remote_class_factory(self.anchor,
                                                                self._local_class,
                                                                self.__class__)

            if not hasattr(self, '_remote'):
                self._remote = self._remote_class(self)
                self._remote._cache = self

            return self._remote

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
        # xattrs failing over to sqlite
        raise NotImplementedError

    @meta.setter
    def meta(self):
        raise NotImplementedError

    @property
    def data(self):
        # we don't keep two copies of the local data
        # unless we are doing a git-like thing
        raise NotImplementedError

    def __repr__(self):
        local = repr(self.local) if self.local else str(self)
        remote = repr(self.remote) if self.remote else self.id
        return local + ' -> ' + remote


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

    @property
    def meta(self):
        # FIXME symlink cache!??!
        if self.is_symlink() and not self.exists():  # if a symlink exists it is something else
            return PathMeta.from_path(self)
        elif self.exists():
            xattrs = self.xattrs()
            pathmeta = PathMeta.from_xattrs(self.xattrs(), self.xattr_prefix, self)
            return pathmeta

    @meta.setter
    def meta(self, pathmeta):
        # TODO cooperatively setting multiple different cache types?
        # do we need to use super() or something?
        if self.is_symlink():
            raise TypeError('FIXME TODO can\'t set meta on a symlink itself')  # FIXME
        else:
            self.setxattrs(pathmeta.as_xattrs(self.xattr_prefix))


class SqliteCache(CachePath):
    """ a persistent store to back up the xattrs if they get wiped """


class BlackfynnCache(SqliteCache, XattrCache):
    xattr_prefix = 'bf'

    @classmethod
    def decode_value(cls, field, value):
        if field in ('created', 'updated'):
            return parser.parse(value.decode())  # FIXME with timezone vs without ...
        else:
            return PathMeta.decode_value(field, value)

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
        if self.meta.id is None:
            parent = self.parent
            if parent == self:  # we have hit a root
                return None

            organization = self.parent.organization

            if organization is not None:
                # TODO repair
                pass
            else:
                raise exc.NotInProjectError()

        if self.meta.id.startswith('N:organization:'):
            return self

        elif self.parent:
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
        id = self.meta.id
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


class LocalPath(PosixPath):
    # local data about remote objects

    _cache_class = None  # must be defined by child classes

    @classmethod
    def setup(cls, cache_class, remote_class_factory):
        """ call this once to bind everything together """
        cache_class.setup(cls, remote_class_factory )

    @property
    def remote(self):
        return self.cache.remote

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
                        id=self.path.as_posx(),  # this is ok, assists in mapping/debug ???
                        uid=st.st_uid,
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
        with open(self, 'wb') as f:
            for chunk in generator:
                f.write(chunk)

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

    def annotations_to_remote(self):
        raise NotImplementedError

    def to_remote(self):
        # FIXME in theory we could have an endpoint for each of these
        # The remote will handle that?
        # this can definitely fail
        self.remote.send(self.data, self.metadata, self.annotations)

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
