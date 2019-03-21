import json
import hashlib
import subprocess
from time import sleep
from pathlib import PosixPath, PurePosixPath
from datetime import datetime
from collections import deque
import xattr
import psutil
import sqlite3
from dateutil.parser import parser
from Xlib.display import Display
from Xlib import Xatom
from IPython import embed


class UnhandledTypeError(Exception):
    """ haven't dealt with this yet """


class JEncode(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, tuple):
            return list(obj)
        elif isinstance(obj, deque):
            return list(obj)
        elif isinstance(obj, ProtcParameter):
            return str(obj)
        elif isinstance(obj, OntId):
            return obj.curie + ',' + obj.label

        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


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

    @classmethod
    def from_xattrs(cls, **kwargs):
        """ decoding from bytes """
        pass

    @classmethod
    def from_metastore(cls, **kwargs):
        """ db entry """
        pass

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

    @property
    def hrsize(self):
        def sizeof_fmt(num, suffix=''):
            for unit in ['','K','M','G','T','P','E','Z']:
                if abs(num) < 1024.0:
                    return "%0.0f%s%s" % (num, unit, suffix)
                num /= 1024.0
            return "%.1f%s%s" % (num, 'Yi', suffix)

        return sizeof_fmt(self.size)

    @classmethod
    def encode_value(cls, field, value):
        if isinstance(value, datetime):
            value = value.isoformat().isoformat().replace('.', ',')

        if isinstance(value, int):
            out = bytes(value)
        elif isinstance(value, str):
            out = value.encode(cls.encoding)
        else:
            raise UnhandledTypeError(f'dont know what to do with {value!r}')

        return out

    def as_xattrs(self, prefix=None):
        """ encoding to bytes """
        out = {}
        for field in self.fields:
            value = getattr(self, field)
            if value:
                value_bytes = self.value_to_bytes(field, value)
                if prefix:
                    key = prefix + '.' + field
                else:
                    key = field

                key_bytes = key.encode(self.encoding)
                out[key_bytes] = self.encode_value(field, value)

        return out

    def as_metastore(self):
        """ db entry """  # TODO json blob in sqlite? can it index?
        return ''

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


class RemotePath(PurePosixPath):
    """ Remote data about a local object. """

    # These should be set by RemoteFactory
    _cache_class = None
    _local_class = None

    # we use a PurePath becuase we still want to key off this being local path
    # but we don't want any of the local file system operations to work by accident
    # so for example self.stat should return the remote value not the local value
    # which is what would happen if we used a PosixPath as the base class

    # need a way to pass the session information in independent of the actual path
    # abstractly having remote.data(global_id_for_local, self)
    # should be more than enough, the path object shouldn't need
    # to know that it has a remote id, the remote manager should
    # know that

    def __new__(cls, *args, **kwargs):
        return super().__new__(cls, *args, **kwargs)

    def __init__(self, *args, **kwargs):
        super().__init__()

    @property
    def cache(self):
        # TODO performance check on these
        return self._cache_class(self)

    @property
    def local(self):
        return self._local_class(self)

    @property
    def root(self):
        raise NotImplemented

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
        self.remote_thing.id_from_machine_id_and_path(self)

    @property
    def data(self):
        self.cache.id
        for chunk in chunks:
            yield chunk

    @property
    def meta(self):
        # on blackfynn this is the package id or object id
        self.cache.id

    @meta.setter
    def meta(self, value):
        raise NotImplemented
        return 'yes?'

    @property
    def annotations(self):
        # these are models etc in blackfynn
        yield from []
        raise NotImplemented

    @property
    def children(self):
        # uniform interface for retrieving remote hierarchies decoupled from meta
        raise NotImplemented

    def iterdir(self):
        # I'm guessing most remotes don't support this
        raise NotImplemented

    def glob(self, pattern):
        raise NotImplemented

    def rglob(self, pattern):
        raise NotImplemented


class CachePath(PosixPath):
    """ Local data about remote objects.
        This is where the mapping between the local id (aka path)
        and the remote id lives. In a git-like world this is the
        cache/index/whatever we call it these days """

    _remote_class = None
    _local_class = None

    @property
    def remote(self):
        return self._remote_class(self)

    @property
    def local(self):
        return self._local_class(self)

    @property
    def id(self):
        return self.meta['id']

    # TODO how to toggle fetch from remote to heal?
    @property
    def meta(self):
        # xattrs failing over to sqlite
        raise NotImplemented

    @property
    def data(self):
        # we don't keep two copies of the local data
        # unless we are doing a git-like thing
        raise NotImplemented


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
        # only decode keys ?
        return {k.decode():v for k, v in xattr.get_all(self.as_posix(), namespace=namespace)}


class XattrCache(CachePath, XattrPath):
    xattr_prefix = None


class SqliteCache(CachePath):
    """ a persistent store to back up the xattrs if they get wiped """


class BlackfynnCache(SqliteCache, XattrCache):
    xattr_prefix = 'bf'
    @property
    def meta(self):
        meta = self.xattrs()
        return meta

    @meta.setter
    def meta(self, value):
        self.setxattrs()


class LocalPath(PosixPath):
    # local data about remote objects
    _cache_class = CachePath
    _remote_class = None

    @property
    def remote(self):
        return self._remote_class(self)

    @property
    def cache(self):
        # TODO performance check on these
        return self._cach_class(self)

    @property
    def id(self):
        """ THERE CAN BE ONLY ONE """
        return self.resolve().as_posix()

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
                        id=self.id,  # this is ok, assists in mapping/debug
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

    @property
    def children(self):
        yield from self.iterdir()

    def diff(self):
        pass

    def meta_to_remote(self):
        # pretty sure that we don't wan't this independent of data_to_remote
        # sort of cp vs cp -a and commit date vs author date
        raise NotImplemented
        meta = self.meta
        # FIXME how do we invalidate cache?
        self.remote.meta = meta  # this can super duper fail

    def data_to_remote(self):
        raise NotImplemented

    def annotations_to_remote(self):
        raise NotImplemented

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


class Path(XattrPath, LocalPath):
    """ An augmented path for all the various local needs of the curation process. """

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


class MetaStore:
    """ A local backup against accidental xattr removal """
    attrs = ('bf.id',
             'bf.file_id',
             'bf.size',
             'bf.created_at',
             'bf.updated_at',
             'bf.checksum',
             'bf.error')
    # FIXME horribly inefficient 1 connection per file due to the async code ... :/
    def __init__(self, db_path):
        self.db_path = db_path
        self.setup()

    def conn(self):
        return sqlite3.connect(self.db_path.as_posix())

    def setup(self):
        if not self.db_path.parent.exists():
            self.db_path.parent.mkdir(parents=True)

        sqls = (('CREATE TABLE IF NOT EXISTS fsxattrs '
                 '(path TEXT PRIMARY KEY,'
                 'bf_id TEXT NOT NULL,'
                 'bf_file_id INTEGER,'
                 'bf_size INTEGER,'
                 'bf_created_at DATETIME,'
                 'bf_updated_at DATETIME,'
                 'bf_checksum BLOB,'
                 'bf_error INTEGER);'),
                ('CREATE UNIQUE INDEX IF NOT EXISTS fsxattrs_u_path ON fsxattrs (path);'))
        conn = self.conn()
        with conn:
            for sql in sqls:
                conn.execute(sql)

    def bulk(self, pdict):
        cols = ', '.join(_.replace('.', '_') for _ in self.attrs)
        values_template = ', '.join('?' for _ in self.attrs)
        sql = ('INSERT OR REPLACE INTO fsxattrs '
               f'(path, {cols}) VALUES (?, {values_template})')
        conn = self.conn()
        with conn:
            for path, attrs in pdict.items():
                args = path.as_posix(), *self.convert_attrs(attrs)
                conn.execute(sql, args)

    def remove(self, path):
        sql = 'DELETE FROM fsxattrs WHERE path = ?'
        args = path.as_posix(),
        conn = self.conn()
        with conn:
            return conn.execute(sql, args)

    def convert_attrs(self, attrs):
        for key in self.attrs:
            if key in attrs:
                yield attrs[key]
            else:
                yield None

    def xattrs(self, path):
        sql = 'SELECT * FROM fsxattrs WHERE path = ?'
        args = path.as_posix(),
        conn = self.conn()
        with conn:
            cursor = conn.execute(sql, args)
            values = cursor.fetchone()
            print(values)
            if values:
                keys = [n.replace('_', '.', 1) for n, *_ in cursor.description]
                #print(keys, values)
                return {k:v for k, v in zip(keys, values) if k != 'path' and v is not None}  # skip path itself
            else:
                return {}

    def setxattr(self, path, key, value):
        return self.setxattrs(path, {key:value})

    def setxattrs(self, path, attrs):
        # FIXME skip nulls on replace
        cols = ', '.join(attrs)
        values_template = ', '.join('?' for _ in self.attrs)
        sql = (f'INSERT OR REPLACE INTO fsxattrs (path, {cols}) VALUES (?, {values_template})')
        args = path.as_posix(), *self.convert_attrs(attrs)
        conn = self.conn()
        with conn:
            return conn.execute(sql, args)

    def getxattr(self, path, key):
        if key in self.attrs:
            col = key.replace('.', '_')
            sql = f'SELECT {col} FROM fsxattrs WHERE path = ?'
            args = path.as_posix(),
            conn = self.conn()
            with conn:
                return conn.execute(sql, args)
        else:
            print('WARNING unknown key', key)


def JT(blob):
    def _populate(blob, top=False):
        if isinstance(blob, list) or isinstance(blob, tuple):
            # TODO alternatively if the schema is uniform, could use bc here ...
            def _all(self, l=blob):  # FIXME don't autocomplete?
                keys = set(k for b in l
                           if isinstance(b, dict)
                           for k in b)
                obj = {k:[] for k in keys}
                _list = []
                _other = []
                for b in l:
                    if isinstance(b, dict):
                        for k in keys:
                            if k in b:
                                obj[k].append(b[k])
                            else:
                                obj[k].append(None)

                    elif any(isinstance(b, t) for t in (list, tuple)):
                        _list.append(JT(b))

                    else:
                        _other.append(b)
                        for k in keys:
                            obj[k].append(None)  # super inefficient

                if _list:
                    obj['_list'] = JT(_list)

                if obj:
                    j = JT(obj)
                else:
                    j = JT(blob)

                if _other:
                    #obj['_'] = _other  # infinite, though lazy
                    setattr(j, '_', _other)

                setattr(j, '_b', blob)
                #lb = len(blob)
                #setattr(j, '__len__', lambda: lb)  # FIXME len()
                return j

            def it(self, l=blob):
                for b in l:
                    if any(isinstance(b, t) for t in (dict, list, tuple)):
                        yield JT(b)
                    else:
                        yield b

            if top:
                # FIXME iter is non homogenous
                return [('__iter__', it), ('_all', property(_all))]
            #elif not [e for e in b if isinstance(self, dict)]:
                #return property(id)
            else:
                # FIXME this can render as {} if there are no keys
                return property(_all)
                #obj = {'_all': property(_all),
                       #'_l': property(it),}

                #j = JT(obj)
                #return j

                #nl = JT(obj)
                #nl._list = blob
                #return property(it)

        elif isinstance(blob, dict):
            if top:
                out = [('_keys', tuple(blob))]
                for k, v in blob.items():  # FIXME normalize keys ...
                    nv = _populate(v)
                    out.append((k, nv))
                    #setattr(cls, k, nv)
                return out
            else:
                return JT(blob)

        else:
            if top:
                raise UnhandledTypeError('asdf')
            else:
                @property
                def prop(self, v=blob):
                    return v

                return prop

    def _repr(self, b=blob):  # because why not
        return 'JT(\n' + repr(b) + '\n)'

    #cd = {k:v for k, v in _populate(blob, True)}

    # populate the top level
    cd = {k:v for k, v in ((a, b) for t in _populate(blob, True)
                           for a, b in (t if isinstance(t, list) else (t,)))}
    cd['__repr__'] = _repr
    nc = type('JT' + str(type(blob)), (object,), cd)
    return nc()
