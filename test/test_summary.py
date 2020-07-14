import unittest
from .common import template_root, project_path
from sparcur.curation import Summary
from sparcur.blackfynn_api import FakeBFLocal


class TestSummary(unittest.TestCase):
    def setUp(self):
        project_path.cache.anchorClassHere(remote_init=False)
        project_path._remote_class._api = FakeBFLocal(project_path.cache.id, project_path.cache)
        self.s = Summary(project_path)
        self.s._n_jobs = 1
        self.s.setup(local_only=True)

    def test_data(self):
        self.s.data()
