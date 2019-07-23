import os
import atexit
import subprocess
from pathlib import PurePosixPath, PurePath
from datetime import datetime
import requests
from pexpect import pxssh
from pyontutils.utils import Async, deferred
from sparcur import exceptions as exc
from sparcur.utils import log
from sparcur.core import BlackfynnId, DoiId
from sparcur.paths import PathMeta, RemotePath, CachePath, LocalPath, Path, SshCache, BlackfynnCache
from sparcur.paths import StatResult, _bind_sysid_

from sparcur.blackfynn_api import BFLocal, FakeBFLocal  # FIXME there should be a better way ...
from blackfynn import Collection, DataPackage, Organization, File
from blackfynn import Dataset
from blackfynn.models import BaseNode

from ast import literal_eval


class ReflectiveCachePath(CachePath):
    """ Oh, it's me. """

    @property
    def meta(self):
        return self.local.meta


class RemoteFactory:
    _api_class = None
    @classmethod
    def fromId(cls, identifier, cache_class, local_class):
        # FIXME decouple class construction for identifier binding
        # _api is not required at all and can be bound explicitly later
        api = cls._api_class(identifier)
        self = RemoteFactory.__new__(cls, local_class, cache_class, _api=api)
        self._errors = []
        self.root = self._api.root
        log.debug('When initializing a remote using fromId be sure to set the cache anchor '
                  'before doing anything else, otherwise you will have a baaad time')
        return self

    def ___new__(cls, *args, **kwargs):
        # NOTE this should NOT be tagged as a classmethod
        # it is accessed at cls time already and tagging it
        # will cause it to bind to the original insource parent
        self = super().__new__(cls)#, *args, **kwargs)
        self._errors = []
        return self

    def __new__(cls, local_class, cache_class, **kwargs):
        # TODO use this call to set the remote of local and cache??
        kwargs['_local_class'] = local_class
        kwargs['_cache_class'] = cache_class
        newcls = cls._bindKwargs(**kwargs)
        newcls.__new__ = cls.___new__
        # FIXME klobbering and how to handle multiple?
        local_class._remote_class = newcls
        local_class._cache_class = cache_class
        cache_class._remote_class = newcls
        return newcls

    @classmethod
    def _bindKwargs(cls, **kwargs):
        new_name = cls.__name__.replace('Factory','')
        classTypeInstance = type(new_name,
                                 (cls,),
                                 kwargs)
        return classTypeInstance


class SshRemoteFactory(RemoteFactory, PurePosixPath, RemotePath):
    """ Testing. To be used with ssh-agent.
        StuFiS The stupid file sync. """

    _cache_class = SshCache
    cypher_command = 'sha256sum'
    encoding = 'utf-8'

    _meta = None  # override RemotePath dragnet
    _meta_maker = LocalPath._meta_maker

    sysid = None
    _bind_sysid = classmethod(_bind_sysid_)

    def ___new__(cls, *args, **kwargs):
        # NOTE this should NOT be tagged as a classmethod
        # it is accessed at cls time already and tagging it
        # will cause it to bind to the original insource parent
        _self = PurePosixPath.__new__(cls, *args)  # no kwargs since the only kwargs are for init
        return _self
    
        # TODO this isn't quite working yet due to bootstrapping issues as usual
        if _self.id != cls._cache_anchor.id:
            self = _self.relative_to(_self.anchor)
        else:
            self = PurePosixPath.__new__(cls, '.')  # FIXME make sure this is interpreted correctly ...

        self._errors = []
        return self

    def __new__(cls, cache_anchor, local_class, host):
        # TODO decouple _new from init here as well
        session = pxssh.pxssh(options=dict(IdentityAgent=os.environ.get('SSH_AUTH_SOCK')))
        session.login(host, ssh_config=Path('~/.ssh/config').expanduser().as_posix())
        cls._rows = 200
        cls._cols = 200
        session.setwinsize(cls._rows, cls._cols)  # prevent linewraps of long commands
        session.prompt()
        atexit.register(lambda:(session.sendeof(), session.close()))
        cache_class = cache_anchor.__class__
        newcls = super().__new__(cls, local_class, cache_class,
                               host=host,
                               session=session)
        newcls._uid, *newcls._gids = [int(i) for i in (newcls._ssh('echo $(id -u) $(id -G)')
                                                       .decode().split(' '))]

        newcls._cache_anchor = cache_anchor
        # must run before we can get the sysid, which is a bit odd
        # given that we don't actually sandbox the filesystem
        newcls._bind_sysid()

        return newcls

    def __init__(self, thing_with_id, cache=None):
        if isinstance(thing_with_id, PurePath):
            thing_with_id = thing_with_id.as_posix()

        super().__init__(thing_with_id, cache=cache)

    @property
    def anchor(self):
        return self._cache_anchor.remote
        # FIXME warning on relative paths ...
        # also ... might be convenient to allow
        # setting non-/ anchors, but perhaps for another day
        #return self.__class__('/', host=self.host)

    @property
    def id(self):
        return f'{self.host}:{self.rpath}'
        #return self.host + ':' + self.as_posix()  # FIXME relative to anchor?

    @property
    def rpath(self):
        # FIXME relative paths when the anchor is set differently
        # the anchor will have to be stored as well since there coulde
        # be many possible anchors per host, thus, if an anchor relative
        # identifier is supplied then we need to construct the full path

        # conveniently in this case if self is a fully rooted path then
        # it will overwrite the anchor path
        # TODO make sure that the common path is the anchor ...
        return (self._cache_anchor.remote / self).as_posix()

    def _parts_relative_to(self, remote, cache_parent=None):
        return self.relative_to(remote).parts

    def refresh(self):
        # TODO probably not the best idea ...
        raise NotImplementedError('This baby goes to the network every single time!')

    def access(self, mode):
        """ types are 'read', 'write', and 'execute' """
        try:
            st = self.stat()

        except (PermissionError, FileNotFoundError) as e:
            return False

        r, w, x = 0x124, 0x92, 0x49
        read    = ((r & st.st_mode) >> 2) & (mode == 'read'    or mode == os.R_OK) * x
        write   = ((w & st.st_mode) >> 1) & (mode == 'write'   or mode == os.W_OK) * x
        execute =  (x & st.st_mode)       & (mode == 'execute' or mode == os.X_OK) * x
        current = read + write + execute

        u, g, e = 0x40, 0x8, 0x1
        return (u & current and st.st_uid == self._uid or
                g & current and st.st_gid in self._gids or
                e & current)

    def open(self, mode='wt', buffering=-1, encoding=None,
             errors=None, newline=None):
        if mode not in ('wb', 'wt'):
            raise TypeError('only w[bt] mode is supported')  # TODO ...

        #breakpoint()
        return
        class Hrm:
            session = self.session
            def write(self, value):
                self.session

        #cmd = ['ssh', self.host, f'"cat - > {self.rpath}"']
        #self.session
        #p = subprocess.Popen()

    @property
    def data(self):
        cmd = ['scp', self.id, '/dev/stdout']
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        while True:
            data = p.stdout.read(4096)  # TODO hinting
            if not data:
                break

            yield data

        p.communicate()

    # reuse meta from local
    # def meta (make it easier to search for this)
    meta = LocalPath.meta  # magic

    #def _ssh(self, remote_cmd):
    @classmethod
    def _ssh(cls, remote_cmd):
        #print(remote_cmd)
        if len(remote_cmd) > cls._cols:
            raise exc.CommandTooLongError
        n_bytes = cls.session.sendline(remote_cmd)
        cls.session.prompt()
        raw = cls.session.before
        out = raw[n_bytes + 1:].strip()  # strip once here since we always will
        #print(raw)
        #print(out)
        return out

    def checksum(self):
        remote_cmd = (f'{self.cypher_command} {self.rpath} | '
                      'awk \'{ print $1 }\';')

        return bytes.fromhex(self._ssh(remote_cmd).decode(self.encoding))

    def stat(self):
        remote_cmd = f'stat "{self.rpath}" -c {StatResult.stat_format}'
        out = self._ssh(remote_cmd)
        try:
            return StatResult(out)
        except ValueError as e:
            if out.endswith(b'Permission denied'):
                raise PermissionError(out.decode())

            elif out.endswith(b'No such file or directory'):
                raise FileNotFoundError(out.decode())

            else:
                raise ValueError(out) from e

    def exists(self):
        try:
            st = self.stat()
            return bool(st)  # FIXME
        except FileNotFoundError:  # FIXME there will be more types here ...
            pass

    @property
    def __parent(self):  # no longer needed since we inherit from path directly
        # because the identifiers are paths if we move
        # file.ext to another folder, we treat it as if it were another file
        # at least for this SshRemote path, if we move a file on our end
        # the we had best update our cache
        # if someone else moves the file on the remote, well, then
        # that file simply vanishes since we weren't notified about it
        # if there is a remote transaction log we can replay if there isn't
        # we have to assume the file was deleted or check all the names and
        # hashes of new files to see if it has moved (and not been changed)
        # a move and change without a sync will be bad for us

        # If you have an unanchored path then resolve()
        # always operates under the assumption that the
        # current working directory which I think is incorrect
        # as soon as you start passing unresolved paths around
        # the remote system doesn't know what context you are in
        # so we need to fail loudly
        # basically force people to manually resolve their paths
        return self.__class__(self.cache.parent)  # FIXME not right ...

    def is_dir(self):
        remote_cmd = f'stat -c %F {self.rpath}'
        out = self._ssh(remote_cmd)
        return out == b'directory'

    def is_file(self):
        remote_cmd = f'stat -c %F {self.rpath}'
        out = self._ssh(remote_cmd)
        return out == b'regular file'

    @property
    def children(self):
        # this is amusingly bad, also children_recursive ... drop the maxdepth
        #("find ~/files/blackfynn_local/SPARC\ Consortium -maxdepth 1 "
        #"-exec stat -c \"'%n' %o %s %W %X %Y %Z %g %u %f\" {} \;")
        # chechsums when listing children? maybe ...
        #\"'%n' %o %s %W %X %Y %Z %g %u %f\"
        if self.is_dir():
            # no children if it is a file sadly
            remote_cmd = (f"cd {self.rpath};"
                          f"stat -c {StatResult.stat_format} {{.,}}*;"
                          "echo '----';"
                          f"{self.cypher_command} {{.,}}*;"  # FIXME fails on directories destroying alignment
                          'cd "${OLDPWD}"')

            out = self._ssh(remote_cmd)
            stats, checks = out.split(b'\r\n----\r\n')
            #print(stats)
            stats = {sr.name:sr for s in stats.split(b'\r\n')
                     for sr in (StatResult(s),)}
            checks = {fn:bytes.fromhex(cs) for l in checks.split(b'\r\n')
                      if not b'Is a directory' in l
                      for cs, fn in (l.decode(self.encoding).split('  ', 1),)}

            return stats, checks  # TODO

    def __repr__(self):
        return f'{self.__class__.__name__}({self.rpath!r}, host={self.host!r})'


class BlackfynnRemote(RemotePath):

    uri_human = BlackfynnCache.uri_human
    uri_api = BlackfynnCache.uri_api
    _api_class = BFLocal
    _async_rate = None

    @property
    def errors(self):
        yield from self._errors
        if self.remote_in_error:
            yield 'remote-error'

    @property
    def remote_in_error(self):
        return self.state == 'ERROR'

    @property
    def state(self):
        if not self.is_dir():
            return self.bfobject.state

    @staticmethod
    def get_file(package, file_id):
        files = package.files
        if len(files) > 1:
            log.critical(f'MORE THAN ONE FILE IN PACKAGE {package.id}')
        for file in files:
            if file.id == file_id:
                return file

        else:
            raise FileNotFoundError(f'{package} has no file with id {file_id} but has:\n{files}')

    @classmethod
    def get_file_url(cls, id, file_id):
        if file_id is not None:
            return cls._api.get_file_url(id, file_id)

    @classmethod
    def get_file_by_id(cls, id, file_id):
        url = cls.get_file_url(id, file_id)
        yield from cls.get_file_by_url(url)

    @classmethod
    def get_file_by_url(cls, url):
        """ NOTE THAT THE FIRST YIELD IS HEADERS """
        resp = requests.get(url, stream=True)
        headers = resp.headers
        yield headers
        log.debug(f'reading from {url}')
        for chunk in resp.iter_content(chunk_size=4096):  # FIXME align chunksizes between local and remote
            if chunk:
                yield chunk

    def __init__(self, id_bfo_or_bfr, *, file_id=None, cache=None):
        self._seed = id_bfo_or_bfr
        self._file_id = file_id
        if not [type_ for type_ in (self.__class__,
                                    BaseNode,
                                    str,
                                    PathMeta)
                if isinstance(self._seed, type_)]:
            raise TypeError(self._seed)

        if cache is not None:
            self._cache_setter(cache, update_meta=False)

        self._errors = []

    @property
    def bfobject(self):
        if hasattr(self, '_bfobject'):
            return self._bfobject

        if isinstance(self._seed, self.__class__):
            bfobject = self._seed.bfobject

        elif isinstance(self._seed, BaseNode):
            bfobject = self._seed

        elif isinstance(self._seed, str):
            bfobject = self._api.get(self._seed)

        elif isinstance(self._seed, PathMeta):
            bfobject = self._api.get(self._seed.id)

        else:
            raise TypeError(self._seed)

        if hasattr(bfobject, '_json'):
            # constructed from a packages query
            # which we need in order for things to be fastish
            self._bfobject = bfobject
            return self._bfobject

        if isinstance(bfobject, DataPackage):
            def transfer(file, bfobject):
                file.parent = bfobject.parent
                file.dataset = bfobject.dataset
                file.state = bfobject.state
                file.package = bfobject
                return file

            files = bfobject.files
            parent = bfobject.parent
            if files:
                if self._file_id is not None:
                    for file in files:
                        if file.id == self._file_id:
                            bfobject = transfer(file, bfobject)

                elif len(files) > 1:
                    log.critical(f'MORE THAN ONE FILE IN PACKAGE {bfobject.id}')
                    if (len(set(f.size for f in files)) == 1 and
                        len(set(f.name for f in files)) == 1):
                        log.critical('Why are there multiple files with the same name and size here?')
                        file = files[0]
                        bfobject = transfer(file, bfobject)
                    else:
                        log.critical(f'There are actually multiple files ...\n{files}')

                else:
                    file = files[0]
                    bfobject = transfer(file, bfobject)

                bfobject.parent = parent  # sometimes we will just reset a parent to itself
            else:
                log.warning(f'No files in package {bfobject.id}')

        self._bfobject = bfobject
        return self._bfobject

    def is_anchor(self):
        return self.anchor == self

    @property
    def anchor(self):
        """ NOTE: this is a slight depature from the semantics in pathlib
            because this returns the path representation NOT the string """
        return self.organization

    @property
    def organization(self):
        # organization is the root in this system so
        # we do not want to depend on parent to look this up
        # nesting organizations is file, but we need to know
        # what the 'expected' root is independent of the actual root
        # because if you have multiple virtual trees on top of the
        # same file system then you need to know the root for
        # your current tree assuming that the underlying ids
        # can be reused (as in something like dat)

        if not hasattr(self.__class__, '_organization'):
            self.__class__._organization = self.__class__(self._api.organization)

        return self._organization

    def is_organization(self):
        return isinstance(self.bfobject, Organization)

    def is_dataset(self):
        return isinstance(self.bfobject, Dataset)

    @property
    def dataset_id(self):
        """ save a network transit if we don't need it """
        dataset = self.bfobject.dataset
        if isinstance(dataset, str):
            return dataset
        else:
            return dataset.id

    @property
    def dataset(self):
        dataset = self.bfobject.dataset
        if isinstance(dataset, str):
            dataset = self.organization.get_child_by_id(dataset)
            self.bfobject.dataset = dataset.bfobject
        else:
            dataset = self.__class__(dataset)

        return dataset

    def get_child_by_id(self, id):
        for c in self.children:
            if c.id == id:
                return c

    @property
    def from_packages(self):
        return hasattr(self.bfobject, '_json')

    @property
    def stem(self):
        name = PurePosixPath(self._name)
        return name.stem
        #if isinstance(self.bfobject, File) and not self.from_packages:
            #return name.stem
        #else:
            #return name.stem

    @property
    def suffix(self):
        # fixme loads of shoddy logic in here
        name = PurePosixPath(self._name)
        if isinstance(self.bfobject, File) and not self.from_packages:
            return name.suffix
        elif isinstance(self.bfobject, Collection):
            return ''
        elif isinstance(self.bfobject, Dataset):
            return ''
        elif isinstance(self.bfobject, Organization):
            return ''
        else:
            if hasattr(self.bfobject, 'type'):
                type = self.bfobject.type.lower()  # FIXME ... can we match s3key?
            else:
                type = None

            if type not in ('unknown', 'unsupported', 'generic', 'genericdata'):
                pass

            elif hasattr(self.bfobject, 'properties'):
                for p in self.bfobject.properties:
                    if p.key == 'subtype':
                        type = p.value.replace(' ', '').lower()
                        break

            return ('.' + type) if type is not None else ''

    @property
    def _name(self):
        name = self.bfobject.name
        if isinstance(self.bfobject, File) and not self.from_packages:
            realname = os.path.basename(self.bfobject.s3_key)
            if name != realname:  # mega weirdness
                if realname.startswith(name):
                    name = realname

                else:
                    realpath = PurePath(realname)
                    namepath = PurePath(name)
                    if namepath.suffixes:
                        log.critical('sigh {namepath!r} -?-> {realpath!r}')

                    else:
                        path = namepath
                        for suffix in realpath.suffixes:
                            path = path.with_suffix(suffix)

                        old_name = name
                        name = path.as_posix()
                        log.info(f'name {old_name} -> {name}')

        if '/' in name:
            bads = ','.join(f'{i}' for i, c in enumerate(name) if c == '/')
            self._errors.append(f'slashes {bads}')
            log.critical(f'GO AWAY {self}')
            name = name.replace('/', '_')
            self.bfobject.name = name  # AND DON'T BOTHER US AGAIN

        return name

    @property
    def name(self):
        return self.stem + self.suffix

    @property
    def id(self):
        if isinstance(self._seed, self.__class__):
            id = self._seed.bfobject.id

        elif isinstance(self._seed, BaseNode):
            if isinstance(self._seed, File):
                id = self._seed.pkg_id
            else:
                id = self._seed.id

        elif isinstance(self._seed, str):
            id = self._seed

        elif isinstance(self._seed, PathMeta):
            id = self._seed.id

        else:
            raise TypeError(self._seed)

        return BlackfynnId(id)

    @property
    def doi(self):
        blob = self.bfobject.doi
        print(blob)
        if blob:
            return DoiId(blob['doi'])

    @property
    def size(self):
        if isinstance(self.bfobject, File):
            return self.bfobject.size

    @property
    def created(self):
        if not isinstance(self.bfobject, Organization):
            return self.bfobject.created_at

    @property
    def updated(self):
        if not isinstance(self.bfobject, Organization):
            return self.bfobject.updated_at

    @property
    def file_id(self):
        if isinstance(self.bfobject, File):
            return self.bfobject.id

    @property
    def old_id(self):
        return None

    def is_dir(self):
        bfo = self.bfobject
        return not isinstance(bfo, File) and not isinstance(bfo, DataPackage)

    def is_file(self):
        bfo = self.bfobject
        return (isinstance(bfo, File) or
                isinstance(bfo, DataPackage) and
                (hasattr(bfo, 'fake_files') and bfo.fake_files
                    or
                 not hasattr(bfo, '_json') and
                 not (not log.warning('going to network for files') and self._has_remote_files())
                 # massively inefficient but probably shouldn't get here?
                ))

    def _has_remote_files(self):
        """ this will fetch """
        bfobject = self.bfobject
        if not isinstance(bfobject, DataPackage):
            return False

        files = bfobject.files
        if not files:
            return False

        if len(files) > 1:
            log.critical(f'{self} has more than one file! Not switching bfobject!')
            return True

        file, = files
        file.parent = bfobject.parent
        file.dataset = bfobject.dataset
        file.package = bfobject
        self._bfobject = file
        return True

    @property
    def checksum(self):  # FIXME using a property is inconsistent with LocalPath
        if hasattr(self.bfobject, 'checksum'):
            checksum = self.bfobject.checksum
            if checksum and '-' not in checksum:
                return bytes.fromhex(checksum)

    @property
    def etag(self):
        """ NOTE returns checksum, count since it is an etag"""
        # FIXME rename to etag in the event that we get proper checksumming ??
        if hasattr(self.bfobject, 'checksum'):
            checksum = self.bfobject.checksum
            if checksum and '-' in checksum:
                log.debug(checksum)
                if isinstance(checksum, str):
                    checksum, strcount = checksum.rsplit('-', 1)
                    count = int(strcount)
                    #if checksum[-2] == '-':  # these are 34 long, i assume the -1 is a check byte?
                        #return bytes.fromhex(checksum[:-2])
                    return bytes.fromhex(checksum), count

    @property
    def chunksize(self):
        if hasattr(self.bfobject, 'chunksize'):
            return self.bfobject.chunksize

    @property
    def owner_id(self):
        if not isinstance(self.bfobject, Organization):
            # This seems like an oversight ...
            return self.bfobject.owner_id

    @property
    def parent(self):
        if isinstance(self.bfobject, Organization):
            return None

        elif isinstance(self.bfobject, Dataset):
            return self.organization
            #parent = self.bfobject._api._context

        else:
            parent = self.bfobject.parent
            if parent is None:
                parent = self.bfobject.dataset

        if False and isinstance(parent, str):
            if parent in self.helper_index:
                return self.helper_index[parent]
            else:
                raise TypeError('grrrrrrrrrrrrrrr')

        if parent:
            parent_cache = self.cache.parent if self.cache is not None else None
            return self.__class__(parent, cache=parent_cache)

    @property
    def children(self):
        if isinstance(self.bfobject, File):
            return
        elif isinstance(self.bfobject, DataPackage):
            return  # we conflate data packages and files
        elif isinstance(self.bfobject, Organization):
            for dataset in self.bfobject.datasets:
                child = self.__class__(dataset)
                self.cache / child  # construction will cause registration without needing to assign
                assert child.cache
                yield child
        else:
            for bfobject in self.bfobject:
                child = self.__class__(bfobject)
                self.cache / child  # construction will cause registration without needing to assign
                assert child.cache
                yield child

    @property
    def rchildren(self):
        yield from self._rchildren()

    def _rchildren(self, create_cache=True):
        if isinstance(self.bfobject, File):
            return
        elif isinstance(self.bfobject, DataPackage):
            return  # should we return files inside packages? are they 1:1?
        elif any(isinstance(self.bfobject, t) for t in (Organization, Collection)):
            for child in self.children:
                yield child
                yield from child.rchildren
        elif isinstance(self.bfobject, Dataset):
            for bfobject in self.bfobject.packages:
                child = self.__class__(bfobject)
                if child.is_dir() or child.is_file():
                    if child.is_file():
                        cid = child.id
                        existing = [c for c in self.cache.local.children
                                    if (c.is_file() and c.cache or c.is_broken_symlink())
                                    and c.cache.id == cid]
                        if existing:
                            unmatched = [e for e in existing if child.name != e.name]
                            if unmatched:
                                log.debug(f'skipping {child.name} becuase a file with that '
                                          f'id already exists {unmatched}')
                                continue

                    if create_cache:
                        # FIXME I don't think existing detection is working
                        # correctly here so this get's triggered incorrectly?
                        self.cache / child  # construction will cause registration without needing to assign
                        assert child.cache is not None

                    yield child
                else:
                    # probably a package that has files
                    log.debug(f'skipping {child} becuase it is neither a directory nor a file')
        else:
            raise exc.UnhandledTypeError  # TODO

    def children_pull(self, existing_caches=tuple(), only=tuple(), skip=tuple()):
        # FIXME this is really a recursive pull for organization level only ...
        sname = lambda gen: sorted(gen, key=lambda c: c.name)
        def refresh(c):
            updated = c.meta.updated
            newc = c.refresh()
            if newc is None:
                return

            nupdated = newc.meta.updated
            if nupdated != updated:
                return newc

        existing = sname(existing_caches)
        skipexisting = {e.id:e for e in
                        Async(rate=self._async_rate)(deferred(refresh)(e) for e in existing)
                        if e is not None}

        # FIXME
        # in theory the remote could change betwee these two loops
        # since we currently cannot do a single atomic pull for
        # a set of remotes and have them refresh existing files
        # in one shot

        yield from (rc for d in Async(rate=self._async_rate)(
            deferred(child.bootstrap)(recursive=True, only=only, skip=skip)
            for child in sname(self.children)
            #if child.id in skipexisting
            # TODO when dataset's have a 'anything in me updated'
            # field then we can use that to skip things that haven't
            # changed (hello git ...)
            ) for rc in d)

    def isinstance_bf(self, *types):
        return [t for t in types if isinstance(self.bfobject, t)]

    def refresh(self, update_cache=False, update_data=False,
                update_data_on_cache=False, size_limit_mb=2, force=False):
        """ use force if you have a file from packages """
        try:
            old_meta = self.meta
        except exc.NoMetadataRetrievedError as e:
            log.error(f'{e}\nYou will need to individually refresh {self.local}')
            return
        except exc.NoRemoteFileWithThatIdError as e:
            log.exception(e)
            return

        if self.is_file() and not force:  # this will tigger a fetch
            pass
        else:
            #self._bfo0 = self._bfobject
            self._bfobject = self._api.get(self.id)
            #self._bfo1 = self._bfobject
            self.is_file()  # trigger fetching file in the no file_id case
            #self._bfo2 = self._bfobject

        if update_cache or update_data:
            file_is_different = self.update_cache()
            update_existing = file_is_different and self.cache.exists()
            udoc = update_data_on_cache and file_is_different
            if update_existing or udoc:
                size_limit_mb = None

            update_data = update_data or update_existing or udoc

        if update_data and self.is_file():
            self.cache.fetch(size_limit_mb=size_limit_mb)

        return self.cache  # when a cache calls refresh it needs to know if it no longer exists

    def update_cache(self):
        log.debug(f'maybe updating cache for {self.name}')
        file_is_different = self.cache._meta_updater(self.meta)
        # update the cache first
        # then move to the new name if relevant
        # prevents moving partial metadata onto existing files
        if self.cache.name != self.name:  # this is localy correct
            # the issue is that move is now smarter
            # and will detect if a parent path has changed
            try:
                self.cache.move(remote=self)
            except exc.WhyDidntThisGetMovedBeforeError as e:
                # AAAAAAAAAAAAAAAAAAAAAAAAAAAAA
                # deal with the sadness that is non-unique filenames
                # I am 99.999999999999999% certain that users do not
                # expect this behavior ...
                log.error(e)
                if self.bfobject.package.name != self.bfobject.name:
                    argh = self.bfobject.name
                    self.bfobject.name = self.bfobject.package.name
                    try:
                        log.critical(f'Non unique filename :( '
                                     f'{self.cache.name} -> {argh} -> {self.bfobject.name}')
                        self.cache.move(remote=self)
                    finally:
                        self.bfobject.name = argh
                else:
                    raise e

        return file_is_different

    @property
    def data(self):
        if isinstance(self.bfobject, DataPackage):
            files = list(self.bfobject.files)
            if len(files) > 1:
                raise BaseException('TODO too many files')

            file = files[0]
        elif isinstance(self.bfobject, File):
            file = self.bfobject
        else:
            return

        gen = self.get_file_by_url(file.url)
        self.data_headers = next(gen)
        yield from gen

    @data.setter
    def data(self):
        if hasattr(self, '_seed'):
            # upload the new file
            # delete the old file
            # or move to .trash  self._api.bf.move(target, self.id)
            # where target is the bfobject for .trash
            raise NotImplementedError('TODO')
        else:  # doesn't exist yet
            # see https://github.com/HumanCellAtlas/dcp-cli/pull/252
            # for many useful references
            raise NotImplementedError('TODO')

    def __truediv__(self, other):  # XXX
        """ this is probably the we want to use for this at all
            it is kept around as a reminder NOT to do this
            however might revisit this at some point if we want
            to explore translating remote semantics to file system
            on the RemotePath class ... """
        # probably better to work from the cache class
        # since it is the one that knows that the file doesn't
        # exist at the remote and can provide a way to move data
        # to the remote using copy_to or something like that
        children = list(self.children)
        names = {c.name:c for c in children}
        opath = PurePath(other)
        if len(opath.parts) > 1:
            # FIXME ... handle/paths/like/this
            raise NotImplementedError('TODO')
        else:
            if other in names:
                return names[other]
            else:  # create an empty
                child = object.__new__(self.__class__)
                child._parent = self
                class TempBFObject:
                    name = other
                child._bfobject = TempBFObject()
                return child

    def _mkdir_child(self, child_name):
        """ direct children only for this, call in recursion for multi """
        if self.is_organization():
            bfobject = self._api.bf.create_dataset(child_name)
        elif self.is_dir():  # all other possible dirs are already handled
            bfobject = self.bfobject.create_collection(child_name)
        else:
            raise exc.NotADirectoryError(f'{self}')

        return bfobject

    def mkdir(self, parents=False):  # XXX
        # same issue as with __rtruediv__
        if hasattr(self, '_seed'):
            raise exc.PathExistsError(f'remote already exists {self}')

        bfobject = self._parent._mkdir_child(self.name)
        self._seed = bfobject
        self._bfobject = bfobject

    @property
    def meta(self):
        return PathMeta(size=self.size,
                        created=self.created,
                        updated=self.updated,
                        checksum=self.checksum,
                        etag=self.etag,
                        chunksize=self.chunksize,
                        id=self.id,
                        file_id=self.file_id,
                        old_id=None,
                        gid=None,  # needed to determine local writability
                        user_id=self.owner_id,
                        mode=None,
                        errors=self.errors)

    def __eq__(self, other):
        return self.id == other.id and self.file_id == other.file_id
        #return self.bfobject == other.bfobject

    def __hash__(self):
        return hash((self.__class__, self.id))

    def __repr__(self):
        file_id = f', file_id={self.file_id}' if self.file_id else ''
        return f'{self.__class__.__name__}({self.id!r}{file_id})'


class BlackfynnRemoteFactory(RemoteFactory, BlackfynnRemote):  # XXX soon to be deprecated
    # FIXME helper index should try to cooperate with the packages index?
    def __new__(cls, cache_anchor, local_class):
        if isinstance(cache_anchor, BlackfynnCache):
            try:
                blackfynn_local_instance = BFLocal(cache_anchor.id)
            except (requests.exceptions.ConnectionError, exc.MissingSecretError) as e:
                log.critical(f'Could not connect to blackfynn {e!r}')
                #blackfynn_local_instance = FakeBFLocal(anchor.id, anchor)  # WARNING polutes things!
                blackfynn_local_instance = 'CONNECTION ERROR'

        else:
            raise TypeError(f'{type(cache_anchor)} is not BFLocal or BlackfynnCache!')

        cache_class = cache_anchor.__class__
        self = super().__new__(cls, local_class, cache_class, _api=blackfynn_local_instance)
        cls._cache_anchor = cache_anchor
        self._errors = []
        self.root = self._api.root
        return self


def main():
    from IPython import embed
    embed()


if __name__ == '__main__':
    main()
