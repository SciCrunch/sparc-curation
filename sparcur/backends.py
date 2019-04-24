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
from sparcur.config import local_storage_prefix

from sparcur.blackfynn_api import BFLocal, FakeBFLocal  # FIXME there should be a better way ...
from blackfynn import Collection, DataPackage, Organization, File
from blackfynn import Dataset
from blackfynn.models import BaseNode

from ast import literal_eval


class ReflectiveCachePath(CachePath):
    """ Oh, it's me. """

    @property
    def id(self):
        return self.resolve().as_posix()


class RemoteFactory:
    """ Assumes that Path is a parent. """
    def ___new__(cls, *args, **kwargs):
        # NOTE this should NOT be tagged as a classmethod
        # it is accessed at cls time already and tagging it
        # will cause it to bind to the original insource parent
        return super().__new__(cls)#, *args, **kwargs)

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

    def __new__(cls, anchor, local_class, cache_class):
        if isinstance(anchor, BFLocal):
            raise TypeError('please do not do this anymore ...')
            blackfynn_local_instance = BFLocal
        elif isinstance(anchor, BlackfynnCache):
            try:
                blackfynn_local_instance = BFLocal(anchor.id)
            except (requests.exceptions.ConnectionError, exc.MissingSecretError) as e:
                log.critical('Could not connect to blackfynn')
                blackfynn_local_instance = FakeBFLocal(anchor.id, anchor)

        else:
            raise TypeError(f'{type(anchor_or_bfl)} is not BFLocal or BlackfynnCache!')

        self = super().__new__(cls, local_class, cache_class, bfl=blackfynn_local_instance)
        self.root = self.bfl.organization.id
        return self

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
        
    def __init__(self, id_bfo_or_bfr, *, file_id=None, cache=None, helper_index=None):

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
                    #breakpoint()
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
            raise TypeError('bfobject cannot be None!')

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
            self.errors.append(f'slashes {bads}')
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
        return isinstance(bfo, File) or isinstance(bfo, DataPackage) and not list(self.children)

    @property
    def checksum(self):
        if hasattr(self.bfobject, 'checksum'):
            return self.bfobject.checksum
        # log this downstream, since downstream also knows the file affected
        #elif isinstance(self.bfobject, File):
            #log.warning(f'No checksum for {self}')

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
                breakpoint()
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
                #breakpoint()
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
                self.cache / child  # construction will cause registration without needing to assign
                assert child.cache is not None
                yield child
        else:
            raise exc.UnhandledTypeError  # TODO

    def isinstance_bf(self, *types):
        return [t for t in types if isinstance(self.bfobject, t)]

    def refresh(self, update_cache=False, update_data=False, size_limit_mb=2):
        old_meta = self.meta
        super().refresh()
        if self.isinstance_bf(File):
            file_id = self.meta.file_id
            if file_id:
                package = self.bfl.get(self.id)
                for i, file in enumerate(package.files):
                    if file.id == file_id:
                        #breakpoint()
                        file.parent = package.parent  # NOTE this means our files are not like the others
                        self.bfobject = file

                if i:
                    log.critical(f'MORE THAN ONE FILE IN PACKAGE {package.id}')

            else:
                log.warning('FIXME File refreshing not implemented')
                return

        elif self.isinstance_bf(DataPackage):
            package = self.bfl.get(self.id)
            self.bfobject = package  # for the time being
            files = package.files  # this makes me sad :/ all I needed was the file id and the size >_<
            for i, file in enumerate(files):
                self.bfobject = file

            if i:
                log.critical(f'MORE THAN ONE FILE IN PACKAGE {package.id}')

        elif self.isinstance_bf(Collection):
            self.bfobject = self.bfl.get(self.id)

        # FIXME all of this needs to go in cache._meta_setter which is where the only
        # meaningful difference should be detected and managed
        # alternately it may need to go inbetween or before, becuase
        # after the changes the old cache might not exist anymore
        new_meta = self.meta

        assert new_meta is not old_meta

        object_type = self.bfobject.__class__.__name__
        actions = []
        for k, old_v in old_meta.items():
            new_v = getattr(new_meta, k)
            if old_v is None:
                action = 'new', object_type, k, new_v
                actions.append(action)
                continue  # don't compare missing fields

            if new_v != old_v:
                action = 'changed', object_type, k, old_v, new_v
                actions.append(action)

        # in the fancy version of this that runs as a total proveance store
        # well, that is on actually on the other side ... because this is the
        # code that pulls data, not the code that waits for things that want to push
        # but, this is sort of where one would want to log all the transactions
        log.debug(f'things needing action: {self.name: <60} {actions}')  # TODO  processing actions move change update delete
        changed = False
        if update_cache:
            c, d = new_meta.__reduce__()
            om = c(**d)
            assert self.meta is new_meta
            if self.cache.meta != new_meta:
                changed = True

            if not self.from_packages and self.cache.name != self.name:
                #breakpoint()
                log.debug(f'{self.cache.name} != {self.name}')
                self.cache.move(remote=self)  # move handles updating all the mappings
            else:
                log.debug(f'updating cache {om}')
                self.cache.meta = new_meta

        if changed and update_data and new_meta.size and new_meta.size.mb < size_limit_mb:
            if self.local.is_symlink():
                self.local.unlink()
                self.local.touch()
                self.cache.meta = new_meta

            self.local.data = self.data

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

        resp = requests.get(file.url, stream=True)
        log.debug(f'reading from to {file.url}')
        for chunk in resp.iter_content(chunk_size=4096):  # FIXME align chunksizes between local and remote
            if chunk:
                yield chunk

    @property
    def meta(self):
        # since BFR is a remote it is OK to memoize the meta
        # because we will have to go to the net to get the new version
        # which probably will be implemented as just creating a whole
        # new one of these and switching it out
        if not hasattr(self, '_meta'):
            if self.errors:
                errors = tuple(self.errors)
            else:
                errors = tuple()

            self._meta = PathMeta(size=self.size,
                                  created=self.created,
                                  updated=self.updated,
                                  checksum=self.checksum,
                                  id=self.id,
                                  file_id=self.file_id,
                                  old_id=None,
                                  gid=None,  # needed to determine local writability
                                  user_id=self.owner_id,
                                  mode=None,
                                  errors=errors)

        return self._meta

    def __eq__(self, other):
        return self.bfobject == other.bfobject

    def __repr__(self):
        return f'{self.__class__.__name__}({self.id})'



def main():
    from IPython import embed
    embed()


if __name__ == '__main__':
    main()
