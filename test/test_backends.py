import os
import sys
import unittest
import pytest
from sparcur.paths import Path, BlackfynnCache as BFC, PennsieveCache as PFC, PennsieveDiscoverCache as PDFC
from sparcur.backends import BlackfynnRemote, PennsieveRemote, PennsieveDiscoverRemote
from .common import test_organization, project_path_real as ppr
# for DatasetData
from .common import RDHPN
from sparcur.backends import PennsieveDatasetData


class RemoteHelper:

    _prr = None
    _remote_class = None
    _cache_class = None
    _data_id = None

    # FIXME skip in CI?
    def setUp(self):
        class Cache(self._cache_class):
            pass

        Cache._bind_flavours()

        self.Remote = self._remote_class._new(Cache._local_class, Cache)
        self.Remote.init(test_organization)
        project_path_real = Cache._local_class(self._ppr.as_posix())
        if not project_path_real.exists():  # FIXME this is something of an insane toggle ??
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
        if self._data_id is None:
            dat = list(next(self.project_path.remote.children).data)
        else:
            data_test = [c for c in self.project_path.remote.children if c.id == self._data_id][0]
            dat = list(data_test.data)
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

    _ppr = ppr
    _remote_class = PennsieveRemote
    _cache_class = PFC


class TestPennsieveDiscoverRemote(RemoteHelper, unittest.TestCase):

    _ppr = ppr.parent / PennsieveDiscoverRemote._project_name
    _remote_class = PennsieveDiscoverRemote
    _cache_class = PDFC
    _data_id = '292'

    def test_pull_fetch_validate(self):
        r = self.Remote(self._data_id)
        r.cache.pull_fetch()
        path = r.local
        from sparcur.cli import main
        # avoid duplicate anchoring attempt since cli expects that it
        # is always the top level entry point, must weigh anchor on
        # both remote and cache
        # FIXME pretty sure we shouldn't have to weight anchor on both ...
        self.Remote.weighAnchor()
        self.Remote._cache_class.weighAnchor()
        with path:
            oav = sys.argv
            try:
                sys.argv = ['spc', 'export', '--discover', '-N']
                main()
            finally:
                sys.argv = oav


@pytest.mark.skipif('CI' in os.environ, reason='Requires access to data')
class TestPennsieveDatasetData(RDHPN, unittest.TestCase):

    _nofetch = True
    examples = (
        # int id know to be present in two different orgs
        'N:dataset:ded103ed-e02d-41fd-8c3e-3ef54989da81',
    )

    def test_publishedMetadata(self):
        # had an issue where int ids are not globally unique (duh)
        # but are instead qualified by the org int id so this hits
        # that codepath directly, have to use real data since there
        # is no "fake publish" endpoint right now
        org = self.anchor.remote.bfobject
        iid = org.int_id  # what we need to filter search results by org int id on discover
        datasets = list(self.anchor.remote.children)
        examples = [d for d in datasets if d.id in self.examples]
        derps = [e.bfobject.publishedMetadata for e in examples]
