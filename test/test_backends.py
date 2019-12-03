import os
import unittest
import pytest
from sparcur.paths import Path, BlackfynnCache as BFC
from sparcur.blackfynn_api import BFLocal
from sparcur.backends import BlackfynnRemote
from .common import test_organization, project_path_real


@pytest.mark.skipif('CI' in os.environ, reason='Requires access to data')
class TestBlackfynnRemote(unittest.TestCase):
    # FIXME skip in CI?
    def setUp(self):
        class BlackfynnCache(BFC):
            pass

        BlackfynnCache._bind_flavours()

        self.BlackfynnRemote = BlackfynnRemote._new(Path, BlackfynnCache)
        self.BlackfynnRemote.init(test_organization)
        if not project_path_real.exists():
            self.anchor = self.BlackfynnRemote.dropAnchor(project_path_real.parent)
        else:
            self.anchor = project_path_real.cache
            self.BlackfynnRemote.anchorTo(self.anchor)

        self.project_path = self.anchor.local

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

    def test_parts_relative_to(self):
        root = self.BlackfynnRemote(self.BlackfynnRemote.root)
        assert root.id == self.BlackfynnRemote.root
