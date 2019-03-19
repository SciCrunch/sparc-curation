from sparcur.core import PathMeta, RemotePath
from sparcur.config import local_storage_prefix


class BlackfynnRemoteFactroy(metaclass=HasBlackfynnSession, RemotePath):
    def ___new__(cls, *args, **kwargs):
        # NOTE this should NOT be tagged as a classmethod
        # it is accessed at cls time already and tagging it
        # will cause it to bind to the original insource parent
        return super().__new__(cls)

    def __new__(cls, organization='sparc', local_storage_prefix=local_storage_prefix):
        bfl = BFLocal(organization)
        newcls = cls._bindSession(bfl)
        newcls.__new__ = cls.___new__
        return newcls

    @classmethod
    def _bindSession(cls, bfl):
        new_name = cls.__name__.replace('Factory','')
        classTypeInstance = type(new_name,
                                 (cls,),
                                 dict(bfl=bfl))
        return classTypeInstance

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
