import os
import atexit
import subprocess
from pathlib import PurePosixPath
from datetime import datetime
import requests
from pexpect import pxssh
from sparcur import exceptions as exc
from sparcur.core import log
from sparcur.paths import PathMeta, RemotePath, CachePath, LocalPath, Path, BlackfynnCache
from sparcur.paths import StatResult

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
    """ Assumes that Path is a parent. """
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


class SshRemoteFactory(RemoteFactory, RemotePath):
    """ Testing. To be used with ssh-agent.
        StuFiS The stupid file sync. """

    cypher_command = 'sha256sum'
    encoding = 'utf-8'

    def __new__(cls, local_class, cache_class, host):
        session = pxssh.pxssh(options=dict(IdentityAgent=os.environ.get('SSH_AUTH_SOCK')))
        session.login(host, ssh_config=Path('~/.ssh/config').expanduser().as_posix())
        cls._rows = 200
        cls._cols = 200
        session.setwinsize(cls._rows, cls._cols)  # prevent linewraps of long commands
        session.prompt()
        atexit.register(lambda:(session.sendeof(), session.close()))
        return super().__new__(cls, local_class, cache_class, host=host, session=session)

    def refresh(self):
        # TODO probably not the best idea ...
        raise NotImplementedError('This baby goes to the network every single time!')

    @property
    def id(self):
        # this allows a remapping once
        # otherwise we face chicken and egg problem
        # or rather, in this case, the remote system
        # really doesn't know how we've mapped something
        # locally, I guess it could, but for the implementaiton
        # we don't track that right now
        return self.cache.id

    @property
    def data(self):
        cmd = ['scp', f'{self.host}:{self.cache.id}', '/dev/stdout']
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

    def _ssh(self, remote_cmd):
        #print(remote_cmd)
        if len(remote_cmd) > self._cols:
            raise exc.CommandTooLongError
        n_bytes = self.session.sendline(remote_cmd)
        self.session.prompt()
        raw = self.session.before
        out = raw[n_bytes + 1:].strip()  # strip once here since we always will
        #print(raw)
        #print(out)
        return out

    def checksum(self):
        remote_cmd = (f'{self.cypher_command} {self.cache.id} | '
                      'awk \'{ print $1 }\';')

        return bytes.fromhex(self._ssh(remote_cmd).decode(self.encoding))

    def stat(self):
        remote_cmd = f'stat "{self.cache.id}" -c {StatResult.stat_format}'
        out = self._ssh(remote_cmd)
        return StatResult(out)

    @property
    def parent(self):
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
        return self.__class__(self.cache.parent)

    def is_dir(self):
        remote_cmd = f'stat -c %F {self.cache.id}'
        out = self._ssh(remote_cmd)
        return out == b'directory'

    @property
    def children(self):
        # this is amusingly bad, also children_recursive ... drop the maxdepth
        #("find ~/files/blackfynn_local/SPARC\ Consortium -maxdepth 1 "
        #"-exec stat -c \"'%n' %o %s %W %X %Y %Z %g %u %f\" {} \;")
        # chechsums when listing children? maybe ...
        #\"'%n' %o %s %W %X %Y %Z %g %u %f\"
        if self.is_dir():
            # no children if it is a file sadly
            remote_cmd = (f"cd {self.cache.id};"
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


class BlackfynnRemoteFactory(RemoteFactory, RemotePath):
    # FIXME helper index should try to cooperate with the packages index?

    uri_human = BlackfynnCache.uri_human
    uri_api = BlackfynnCache.uri_api

    def __new__(cls, cache_anchor, local_class):
        if isinstance(cache_anchor, BlackfynnCache):
            try:
                blackfynn_local_instance = BFLocal(cache_anchor.id)
            except (requests.exceptions.ConnectionError, exc.MissingSecretError) as e:
                log.critical(f'Could not connect to blackfynn {e!r}')
                #blackfynn_local_instance = FakeBFLocal(anchor.id, anchor)  # WARNING polutes things!

        else:
            raise TypeError(f'{type(cache_anchor)} is not BFLocal or BlackfynnCache!')

        cache_class = cache_anchor.__class__
        self = super().__new__(cls, local_class, cache_class, bfl=blackfynn_local_instance)
        cls._cache_anchor = cache_anchor
        self._errors = []
        self.root = self.bfl.organization.id
        return self

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
            return cls.bfl.get_file_url(id, file_id)

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
        if isinstance(id_bfo_or_bfr, self.__class__):
            bfobject = id_bfo_or_bfr.bfobject

        elif isinstance(id_bfo_or_bfr, BaseNode):
            bfobject = id_bfo_or_bfr

        elif isinstance(id_bfo_or_bfr, str):
            bfobject = self.bfl.get(id_bfo_or_bfr)

        elif isinstance(id_bfo_or_bfr, PathMeta):
            bfobject = self.bfl.get(id_bfo_or_bfr.id)

        else:
            raise TypeError(id_bfo_or_bfr)

        if hasattr(bfobject, '_json'):
            # constructed from a packages query
            # which we need in order for things to be fastish
            self.bfobject = bfobject
            return

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
                if file_id is not None:
                    for file in files:
                        if file.id == file_id:
                            bfobject = transfer(file, bfobject)
                            
                elif len(files) > 1:
                    log.critical(f'MORE THAN ONE FILE IN PACKAGE {package.id}')
                else:
                    file = files[0]
                    bfobject = transfer(file, bfobject)

                bfobject.parent = parent  # sometimes we will just reset a parent to itself
            else:
                log.warning(f'No files in package {package.id}')

        self.bfobject = bfobject
        if cache is not None:
            self._cache_setter(cache)

    def __dead_init__(self):
        return
        self.errors = []
        if helper_index is not None:
            self.helper_index.update(helper_index)

        # set _cache to avoid the id equality check since this is in __init__
        if cache is not None:
            self._cache = cache

        if isinstance(id_bfo_or_bfr, self.__class__):
            bfobject = id_bfo_or_bfr.bfobject
        elif isinstance(id_bfo_or_bfr, BaseNode):
            bfobject = id_bfo_or_bfr
            if isinstance(bfobject, DataPackage):  # FIXME :/
                files = bfobject.files
                parent = bfobject.parent
                if files:
                    if len(files) > 1:
                        log.critical(f'MORE THAN ONE FILE IN PACKAGE {package.id}')
                    else:
                        bfobject = files[0]
                        bfobject.parent = parent
                else:
                    log.warning(f'No files in package {package.id}')

        elif id_bfo_or_bfr.startswith('N:organization:'):
            # FIXME right now I require a 1:1 mapping between
            # each local object, cache object, and remote object
            # as a part of the _implementation_, so we do have to
            # be able to duplicate this one, this is OK
            # this is an optimization which we don't strictly have
            # to adhere to, and has caused a lot of trouble :/
            #if hasattr(self.__class__, '_organization'):
                #raise TypeError('You already have a perfectly good organization')

            if id_bfo_or_bfr == self.root:
                if self.cache is None:
                    raise ValueError('where is your cache m8 you are root!')

                self.__class__._organization = self
                self.bfobject = self.bfl.organization
                return

        else:
            #raise ValueError(f'why are you doing this {id_bfo_or_bfr}')  # because refresh
            bfobject = self.bfl.get(id_bfo_or_bfr)
            if isinstance(bfobject, DataPackage):
                if file_id is not None:
                    bfobject = self.get_file(self, file_id)
                else:
                    files = bfobject.files
                    if files:
                        if len(files) > 1:
                            log.critical(f'MORE THAN ONE FILE IN PACKAGE {package.id}')
                        else:
                            bfobject = files[0]
                    else:
                        log.warning(f'No files in package {package.id}')

            elif file_id is not None:
                raise TypeError(f'trying to get file id for a {bfobject}')

        if bfobject is None:
            raise TypeError(f'bfobject cannot be None! {id_bfo_or_bfr}')

        elif isinstance(bfobject, str):
            raise TypeError(f'bfobject cannot be str! {bfobject}')

        elif isinstance(bfobject.id, int):
            pass  # it's a file object so don't check its id as a string

        elif bfobject.id.startswith('N:organization:') and hasattr(self.__class__, '_organization'):
            raise TypeError('You already have a perfectly good organization')

        self.bfobject = bfobject

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
            self.__class__._organization = self.__class__(self.bfl.organization)

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
                name = realname

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

    def __old_name(self):
        if isinstance(self.bfobject, File) and not self.from_packages:
            return PurePosixPath(File.name).stem
        else:
            return 

        if ([t for t in (DataPackage, File) if isinstance(self.bfobject, t)] and
            self.bfobject.type != 'Unknown'):
            # NOTE we have to use blackfynns extensions until we retrieve the files
            name += suffix

        return name

    @property
    def id(self):
        if isinstance(self.bfobject, File):
            return self.bfobject.pkg_id
        else:
            return self.bfobject.id

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
        self.bfobject = file
        return True

    @property
    def checksum(self):
        if hasattr(self.bfobject, 'checksum'):
            checksum = self.bfobject.checksum
            if checksum:
                log.debug(checksum)
                if isinstance(checksum, str):
                    checksum, hrm = checksum.rsplit('-', 1)
                    #if checksum[-2] == '-':  # these are 34 long, i assume the -1 is a check byte?
                        #return bytes.fromhex(checksum[:-2])
                    return bytes.fromhex(checksum)

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
                    self.cache / child  # construction will cause registration without needing to assign
                    assert child.cache is not None
                    yield child
                else:
                    # probably a package that has files
                    log.debug(f'skipping {child} becuase it is neither a directory nor a file')
        else:
            raise exc.UnhandledTypeError  # TODO

    def isinstance_bf(self, *types):
        return [t for t in types if isinstance(self.bfobject, t)]

    def refresh(self, update_cache=False, update_data=False, size_limit_mb=2, force=False):
        """ use force if you have a file from packages """
        old_meta = self.meta
        if self.is_file() and not force:  # this will tigger a fetch
            pass
        else:
            self.bfobject = self.bfl.get(self.id)

        if update_cache:
            #cmeta = self.cache.meta
            log.info(self.name)
            if self.cache.name != self.name:
                self.cache.move(remote=self)

            self.cache._meta_setter(self.meta)

            if update_data and self.is_file():
                self.cache.fetch(size_limit_mb=size_limit_mb)

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

    @property
    def meta(self):
        return PathMeta(size=self.size,
                        created=self.created,
                        updated=self.updated,
                        checksum=self.checksum,
                        id=self.id,
                        file_id=self.file_id,
                        old_id=None,
                        gid=None,  # needed to determine local writability
                        user_id=self.owner_id,
                        mode=None,
                        errors=self.errors)

    def __eq__(self, other):
        return self.bfobject == other.bfobject

    def __repr__(self):
        file_id = f', file_id={self.file_id}' if self.file_id else ''
        return f'{self.__class__.__name__}({self.id!r}{file_id})'



def main():
    from IPython import embed
    embed()


if __name__ == '__main__':
    main()
