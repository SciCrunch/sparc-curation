import unittest
from socket import gethostname
from sparcur.core import Path, BlackfynnCache
from sparcur.blackfynn_api import BFLocal
from sparcur.backends import SshRemoteFactory, BlackfynnRemoteFactory, ReflectiveCachePath

class TestSshRemote(unittest.TestCase):
    def setUp(self):
        hostname = gethostname()
        try:
            self.SshRemote = SshRemoteFactory(Path, ReflectiveCachePath, hostname)
        except TypeError:  # pxssh fail
            self.SshRemote = SshRemoteFactory(Path, ReflectiveCachePath, hostname + '-local')
        self.this_file_darkly = self.SshRemote(__file__)

    def test_checksum(self):
        assert self.this_file_darkly.meta.checksum == self.this_file_darkly.local.meta.checksum

    def test_meta(self):
        #hrm = this_file_darkly.meta.__dict__, this_file_darkly.local.meta.__dict__
        #assert hrm[0] == hrm[1]
        assert self.this_file_darkly.meta == self.this_file_darkly.local.meta

    def test_data(self):
        assert list(self.this_file_darkly.data) == list(self.this_file_darkly.local.data)

    #stats, checks = this_file_darkly.parent.children  # FIXME why does this list the home directory!?


class TestBlackfynnRemote(unittest.TestCase):
    # FIXME skip in CI?
    bfl = BFLocal()
    def setUp(self):
        from sparcur.curation import project_path
        self.project_path = project_path  # FIXME common overwrites?
        self.BlackfynnRemote = BlackfynnRemoteFactory(Path, BlackfynnCache, self.bfl)

    def test_org(self):
        self.project_path.meta
        self.project_path.cache.meta
        self.project_path.remote.meta

        dsl = list(self.project_path.children)
        dsr = list(self.project_path.remote.children)

    def test_data(self):
        dat = list(next(next(self.project_path.remote.children).children).data)
        #list(dd.data) == list(dd.remote.data)

    def test_children(self):
        b = next(next(self.project_path.remote.children).children)
        b.name

    def __test_boot(self):
        # TODO change local storage prefix on bfl
        # and then do a smallish bootstrap
        bfl = BFLocal(local_storage_prefix='/tmp/test_bf_boot')
        BlackfynnRemote = BlackfynnRemoteFactory(Path, BlackfynnCache, bfl)
        project_path = bfl.project_path
        list(project_path.remote.rchildren)
