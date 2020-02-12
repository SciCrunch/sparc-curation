import unittest
from .common import template_root, project_path
from sparcur.curation import Summary


class TestSummary(unittest.TestCase):
    def setUp(self):
        self.s = Summary(project_path)
        self.s._n_jobs = 1
        self.s.setup(local_only=True)

    def test_data(self):
        self.s.data()
