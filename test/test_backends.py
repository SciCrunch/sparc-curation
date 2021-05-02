import os
import unittest
import pytest
from sparcur.paths import Path, BlackfynnCache as BFC, PennsieveCache as PFC
from sparcur.backends import BlackfynnRemote, PennsieveRemote
from .common import test_organization, project_path_real


class RemoteHelper:

    _remote_class = None
    _cache_class = None

    # FIXME skip in CI?
    def setUp(self):
        class Cache(self._cache_class):
            pass

        Cache._bind_flavours()

        self.Remote = self._remote_class._new(Path, Cache)
        self.Remote.init(test_organization)
        if not project_path_real.exists():
            self.anchor = self.Remote.dropAnchor(project_path_real.parent)
        else:
            self.anchor = project_path_real.cache
            self.Remote.anchorTo(self.anchor)

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
        root = self.Remote(self.Remote.root)
        assert root.id == self.Remote.root


@pytest.mark.skipif('CI' in os.environ, reason='Requires access to data')
class TestPennsieveRemote(RemoteHelper, unittest.TestCase):

    _remote_class = PennsieveRemote
    _cache_class = PFC
