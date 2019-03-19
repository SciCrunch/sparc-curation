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

    stat_format = f'\"\'%n\' %o %s %W %X %Y %Z %g %u %f\"'
    cypher_command = 'sha256sum'

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
        # TODO the python version for local too ...
        # plus why not be able to generate this for
        # a whole directory
        remote_cmd = (f'{self.cypher_command} {self.cache.id} | '
                      'awk \'{ printf $1 }\';'
                      "printf ' ';"
                      f'stat "{self.cache.id}" -c {self.stat_format}')

        out = self._ssh(remote_cmd)
        return self._meta(*out.decode().split())

    def _ssh(self, remote_cmd):
        print(remote_cmd)
        cmd = ['ssh', self.host, remote_cmd]
        # FIXME TODO open an ssh session and keep it alive???
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=False)
        out, _ = p.communicate()
        #(checksum, name, hint, size,
        #birth, access, modified, changed,
        #gid, uid, raw_mode)
        return out

    def _meta(self, checksum, name, hint, size,
              birth, access, modified, changed,
              gid, uid, raw_mode):

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
        # chechsums when listing children? maybe ...
        #\"'%n' %o %s %W %X %Y %Z %g %u %f\"
        if self.is_dir():
            # no children if it is a file sadly
            remote_cmd = (f"cd {self.cache.id};"
                        f"stat -c {self.stat_format} {{.,}}*;"
                        "echo '----';"
                        f"{self.cypher_command} {{.,}}* |"  # FIXME fails on directories destroying alignment
                        "awk '{ printf(\"%s\\n\", $1) }'")

            out = self._ssh(remote_cmd)
            out = out.decode()
            stats, checks = out.split('----\n')
            for s, c in zip(stats.split('\n'), checks.split('\n')):
                print(c, s)

            #return self._meta(*out.decode().split())


class BlackfynnRemoteFactroy(RemoteFactory, RemotePath):
    def __new__(cls, organization='sparc', local_storage_prefix=local_storage_prefix):
        bfl = BFLocal(organization)
        return super().__new__(cls, bfl=bfl)

    @property
    def root(self):
        # keep this simple
        # the local paths know where their root is
        # the remote paths know theirs
        # making sure they match requires they both know
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
    assert this_file_darkly.meta.checksum == this_file_darkly.local.checksum().hex()
    this_file_darkly.children  # FIXME why does this list the home directory!?
    embed()


if __name__ == '__main__':
    main()
