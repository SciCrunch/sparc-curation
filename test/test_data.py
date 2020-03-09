import unittest
from pyontutils.core import OntResIri


class TestCurationExportTtl(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ori = OntResIri('https://cassava.ucsd.edu/sparc/exports/curation-export.ttl')
        cls.graph = cls.ori.graph

    def test(self):
        """ sparql queries here """
        self.graph
