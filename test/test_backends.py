import os
import unittest
import pytest
from sparcur.paths import Path, BlackfynnCache
from sparcur.blackfynn_api import BFLocal
from sparcur.backends import BlackfynnRemoteFactory
from .common import project_path


@pytest.mark.skipif('CI' in os.environ, reason='Requires access to data')
class TestBlackfynnRemote(unittest.TestCase):
    # FIXME skip in CI?
    def setUp(self):
        Path._cache_class = BlackfynnCache
        self.project_path = Path(project_path)  # FIXME common overwrites?
        self.anchor = self.project_path.cache
        BlackfynnCache.setup(Path, BlackfynnRemoteFactory)
        self.anchor.remote  # trigger creation of _remote_class
        self.BlackfynnRemote = BlackfynnCache._remote_class

    def test_org(self):
        self.project_path.meta
        self.project_path.cache.meta
        self.project_path.remote.meta

        dsl = list(self.project_path.children)
        dsr = list(self.project_path.remote.children)

    def test_data(self):
        #dat = list(next(next(self.project_path.remote.children).children).data)
        dat = list(next(self.project_path.remote.children).data)
        #list(dd.data) == list(dd.remote.data)

    def test_children(self):
        #b = next(next(self.project_path.remote.children).children)
        b = next(self.project_path.remote.children)
        b.name

    def __test_boot(self):
        # TODO change local storage prefix on bfl
        # and then do a smallish bootstrap
        bfl = BFLocal(local_storage_prefix='/tmp/test_bf_boot')
        BlackfynnRemote = BlackfynnRemoteFactory(Path, BlackfynnCache, bfl)
        project_path = bfl.project_path
        list(project_path.remote.rchildren)

    def test_parts_relative_to(self):
        root = self.BlackfynnRemote(self.BlackfynnRemote.root)
        assert root.id == self.BlackfynnRemote.root
