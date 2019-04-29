import unittest
from .common import template_root, project_path
from sparcur.curation import Summary


class TestSummary(unittest.TestCase):
    def setUp(self):
        self.s = Summary(project_path)

    def test_foundary(self):
        self.s.foundary

    def test_disco(self):
        self.s.disco

    def test_ttl(self):
        self.s.ttl
