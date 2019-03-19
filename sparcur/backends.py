import subprocess
from datetime import datetime
from sparcur.core import PathMeta, RemotePath, CachePath, LocalPath, Path
from sparcur.config import local_storage_prefix
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
        return super().__new__(cls, *args, **kwargs)

    def __new__(cls, local_class, cache_class, **kwargs):
        # TODO use this call to set the remote of local and cache??
        kwargs['_local_class'] = local_class
        kwargs['_cache_class'] = cache_class
        newcls = cls._bindSession(**kwargs)
        newcls.__new__ = cls.___new__
        return newcls

    @classmethod
    def _bindSession(cls, **kwargs):
        new_name = cls.__name__.replace('Factory','')
        classTypeInstance = type(new_name,
                                 (cls,),
                                 kwargs)
        return classTypeInstance


class SshRemoteFactory(RemoteFactory, RemotePath):
    """ Testing. To be used with ssh-agent.
        StuFiS The stupid file sync. """

    def __new__(cls, local_class, cache_class, host):
        return super().__new__(cls, local_class, cache_class, host=host)

    @property
    def data(self):
        cmd = ['scp', f'{self.host}:{self.cache.id}', '/dev/stdout']
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        out, _ = p.communicate()
        return out

    @property
    def meta(self):
        """ stat format
        %a     access rights in octal (note '#' and '0' printf flags)
        %A     access rights in human readable form
        %b     number of blocks allocated (see %B)
        %B     the size in bytes of each block reported by %b
        %C     SELinux security context string
        %d     device number in decimal
        %D     device number in hex
        %f     raw mode in hex
        %F     file type
        %g     group ID of owner
        %G     group name of owner
        %h     number of hard links
        %i     inode number
        %m     mount point
        %n     file name
        %N     quoted file name with dereference if symbolic link
        %o     optimal I/O transfer size hint
        %s     total size, in bytes
        %t     major device type in hex, for character/block device special files
        %T     minor device type in hex, for character/block device special files
        %u     user ID of owner
        %U     user name of owner
        %w     time of file birth, human-readable; - if unknown
        %W     time of file birth, seconds since Epoch; 0 if unknown
        %x     time of last access, human-readable
        %X     time of last access, seconds since Epoch
        %y     time of last data modification, human-readable
        %Y     time of last data modification, seconds since Epoch
        %z     time of last status change, human-readable
        %Z     time of last status change, seconds since Epoch
        Valid format sequences for file systems:
        %a     free blocks available to non-superuser
        %b     total data blocks in file system
        %c     total file nodes in file system
        %d     free file nodes in file system
        %f     free blocks in file system
        %i     file system ID in hex
        %l     maximum length of filenames
        %n     file name
        %s     block size (for faster transfers)
        %S     fundamental block size (for block counts)
        %t     file system type in hex
        %T     file system type in human readable form """

        # TODO the python version for local too ...
        # plus why not be able to generate this for
        # a whole directory
        format = f'\"%n %o %s %W %X %Y %Z %g %u %f\"'
        cypher_command = 'sha256sum'
        remote_cmd = (f'{cypher_command} {self.cache.id} | '
                      'awk \'{ printf $1 }\';'
                      "printf ' ';"
                      f'stat "{self.cache.id}" -c {format}')

        print(remote_cmd)
        cmd = ['ssh', self.host, remote_cmd]
        # FIXME TODO open an ssh session and keep it alive???
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=False)
        out, _ = p.communicate()
        (checksum, name, hint, size,
         birth, access, modified, changed,
         gid, uid, raw_mode) = out.decode().split()
        octal_mode = oct(literal_eval('0x' + raw_mode))

        created = None if birth == '-' else datetime.fromtimestamp(int(birth))
        updated = datetime.fromtimestamp(int(modified))
        return PathMeta(size=size,
                        created=created,
                        updated=modified,
                        checksum=checksum,  # FIXME bytes ...
                        id=self.cache.id,  # FIXME
                        gid=int(gid),
                        uid=int(uid),
                        mode=octal_mode,  # FIXME
        )

    @property
    def children(self):
        # this is amusingly bad, also children_recursive ... drop the maxdepth
        #("find ~/files/blackfynn_local/SPARC\ Consortium -maxdepth 1 "
         #"-exec stat -c \"'%n' %o %s %W %X %Y %Z %g %u %f\" {} \;")

        remote_cmd = "cd {self.cache.id}; stat -c \"'%n' %o %s %W %X %Y %Z %g %u %f\" {.,}*"


class BlackfynnRemoteFactroy(RemoteFactory, RemotePath):
    def __new__(cls, organization='sparc', local_storage_prefix=local_storage_prefix):
        bfl = BFLocal(organization)
        return super().__new__(cls, bfl=bfl)

    @property
    def root(self):
        # keep this simple
        # the local paths know where their root is
        # the remote paths know theirs
        # making sure they match requris they both know
        # their own roots first
        return self.bfl.bf.organization.id

    @property
    def id(self):
        raise NotImplemented('The blackfynn remote cannot answer this question.')

    @property
    def data(self):
        # call on folder does nothing?
        # or what? I guess we could fetch everything
        # but that is definitely dangerous default behavior ...
        #if self.is_file():
            #self.cache.id

        return None

    @property
    def meta(self):
        hbfo = self.bfl.get_homogenous(self.cache.id)
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
        return None


def main():
    from IPython import embed
    from socket import gethostname
    SshRemote = SshRemoteFactory(Path, ReflectiveCachePath, gethostname())
    this_file_darkly = SshRemote(__file__)
    embed()


if __name__ == '__main__':
    main()
