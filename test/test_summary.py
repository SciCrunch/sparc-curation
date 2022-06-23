import unittest
from sparcur.curation import Summary
from sparcur.pennsieve_api import FakeBFLocal
from .common import skipif_no_net, skipif_ci
from .common import template_root, project_path


@skipif_ci
@skipif_no_net
class TestSummary(unittest.TestCase):
    def setUp(self):
        try:
            project_path.cache.anchorClassHere(remote_init=False)
        except ValueError as e:
            # already anchored hopefully, but if not we'll find out soon!
            pass

        project_path._remote_class._api = FakeBFLocal(project_path.cache.id, project_path.cache)
        self.s = Summary(project_path)
        self.s._n_jobs = 1
        self.s.setup(local_only=True)

    def test_data(self):
        self.s.data()
