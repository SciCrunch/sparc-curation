import os
import atexit
import subprocess
from datetime import datetime
from pexpect import pxssh
from sparcur.paths import PathMeta, RemotePath, CachePath, LocalPath, Path
from sparcur.config import local_storage_prefix
from sparcur.blackfynn_api import HomogenousBF  # FIXME there should be a better way ...
from ast import literal_eval


class CommandTooLongError(Exception):
    """ not the best solution ... """


class NoRemoteImplementationError(Exception):
    """ prevent confusion between local path data and remote path data """


class NoRemoteMappingError(Exception):
    """ prevent confusion between local path data and remote path data """


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
        return super().__new__(cls, *args, **kwargs)

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


class StatResult:
    stat_format = f'\"%n  %o  %s  %w  %W  %x  %X  %y  %Y  %z  %Z  %g  %u  %f\"'

    #stat_format = f'\"\'%n\' %o %s \'%w\' %W \'%x\' %X \'%y\' %Y \'%z\' %Z %g %u %f\"'

    def __init__(self, out):
        out = out.decode()
        #name, rest = out.rsplit("'", 1)
        #self.name = name.strip("'")
        #print(out)
        wat = out.split('  ')
        #print(wat)
        #print(len(wat))
        name, hint, size, hb, birth, ha, access, hm, modified, hc, changed, gid, uid, raw_mode = wat

        self.name = name

        def ns(hr):
            date, time, zone = hr.split(' ')
            time, ns = time.split('.')
            return '.' + ns

        self.st_blksize = int(hint)
        self.st_size = int(size)
        #self.st_birthtime
        self.st_atime = float(access + ns(ha)) 
        self.st_mtime = float(modified + ns(hm))
        self.st_ctime = float(changed + ns(hc))
        self.st_gid = int(gid)
        self.st_uid = int(uid)
        self.st_mode = int.from_bytes(bytes.fromhex(raw_mode), 'big')

        if hb != '-' and birth != '0':
            self.st_birthtime = float(birth + ns(hb))


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
    meta = LocalPath.meta  # magic

    def _ssh(self, remote_cmd):
        #print(remote_cmd)
        if len(remote_cmd) > self._cols:
            raise CommandTooLongError
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
        return self.__class__(self.cache.parent.resolve())

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
    def __new__(cls, local_class, cache_class, blackfynn_local_instance):
        # TODO bootstrap root from local_storage_prefix???
        #if bfl.organization.id != 
        #organization='sparc', local_storage_prefix=local_storage_prefix)
        #bfl = BFLocal(organization)
        return super().__new__(cls, local_class, cache_class, bfl=blackfynn_local_instance)

    def bootstrap_local(self, *, fetch_data=False, id=None, parents=False):
        if hasattr(self, '_bfobject') and self.is_file():
            if self.meta.size is not None:
                fetch_data = self.meta.size <  2 * 1024 ** 2

        super().bootstrap_local(fetch_data=fetch_data, id=id, parents=parents)

    @property
    def root(self):
        # keep this simple
        # the local paths know where their root is
        # the remote paths know theirs
        # making sure they match requires they both know
        # their own roots first
        return self.bfl.organization.id

    def _id(self):
        raise NotImplementedError('The blackfynn remote cannot answer this question.')

    @property
    def id(self):
        if hasattr(self, '_bootstrap') and self._bootstrap:
            return self._id
        else:
            return self._id()

    @id.setter
    def id(self, value):
        self._bootstrap = True
        self._id = value

    def is_dir(self):
        return not self.is_file()

    def is_file(self):
        return self.meta.id.startswith('N:package:')

    @property
    def data(self):
        # call on folder does nothing?
        # or what? I guess we could fetch everything
        # but that is definitely dangerous default behavior ...
        #if self.is_file():
            #self.cache.id
        yield from self.hbfo.data

    @property
    def bfobject(self):
        """ conventional name to retrieve whatever the native remote representation is """
        # caching makes sense here since local and cache paths create a new instance
        # every time they reference remote again, if they want to keep a local copy
        # they can for synchronization purposes, but remote really does got and get
        # things again when you create a new one
        if not hasattr(self, '_bfobject'):
            if hasattr(self, '_bootstrap') and self._bootstrap:
                id = self.id
            else:
                id = self.cache.id

            self._bfobject = self.bfl.get(id)

        return self._bfobject

    @property
    def hbfo(self):
        return HomogenousBF(self.bfobject)

    @property
    def checksum(self):
        """ inefficient for this """
        return NotImplemented('Inefficient to query directly, use meta.checksum')

    @property
    def stat(self):
        """ inefficient for this """
        return NotImplemented('Inefficient to query directly, use meta')

    @property
    def meta(self):
        # if id
        # if parent id recursive
        hbfo = self.hbfo  # FIXME files vs packages, they have different updated/created
        return PathMeta(size=hbfo.size,
                        created=hbfo.created,
                        updated=hbfo.updated,
                        checksum=hbfo.checksum,
                        id=hbfo.id,
                        file_id=hbfo.file_id,
                        old_id=None,
                        gid=None,  # needed to determine local writability
                        uid=None,
                        mode=None,
                        error=None)

    @property
    def children(self):
        """ direct children """
        for child in self.hbfo.children:
            child_path = self / child.name
            child_path._bfobject = child.bfobject
            yield child_path

    @property
    def rchildren(self):
        for child in self.hbfo.rchildren:
            # FIXME need to stop a p.name == self.name
            args = (*[p.name for p in child.rparents], child.name)
            print(self.__class__, self, args)
            child_path = self.__class__(self, *args)
            child_path._bfobject = child.bfobject
            yield child_path

        return
        id = self.cache.id
        if id:
            if self.cache:
                # FIXME make sure org ids match?!
                #if id != self.bfl.organization.id:
                    
                for d in self.bfl.datasets:
                    yield d
                    yield from bfl.get_packages()
        else:
            # NOTE cache manages the robustness layer
            raise NoRemoteMappingError(f'{self} has no known remote id')
        # if organization
        # if dataset (usually going to be the fastest in most cases)
        # if collection (can end up very slow)
        # if package/file

def main():
    from IPython import embed
    embed()


if __name__ == '__main__':
    main()
