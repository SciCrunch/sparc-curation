import unittest
from socket import gethostname
from sparcur.core import Path
from sparcur.backends import SshRemoteFactory, BlackfynnRemoteFactroy, ReflectiveCachePath

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
