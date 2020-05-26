import unittest
from pathlib import Path
import pytest
from sparcur import apinat
from sparcur import pipelines as pipes
from .common import examples_root


class TestApiNAT(unittest.TestCase):

    source = Path(__file__).parent / 'apinatomy'

    def test_load(self):
        m = pipes.ApiNATOMY(Path(self.source, 'keast-spinal-map.json'))
        # FIXME I think only the model conforms to the schema ?
        #g = pipes.ApiNATOMY(Path(self.source, 'apinatomy-generated.json'))
        #rm = pipes.ApiNATOMY(Path(self.source, 'apinatomy-resourceMap.json'))
        m.data.keys()
        assert 'resources' in m.data, m.data

    def test_export(self):
        m = pipes.ApiNATOMY(self.source / 'keast-spinal-map.json')
        # FIXME need a way to combine this that doesn't require
        # the user to know how to compose these, just send a message
        # to one of them they should be able to build the other from
        # the information at hand
        r = pipes.ApiNATOMY_rdf(m)
        graph = r.data
        assert list(graph[:apinat.readable.name:]), graph.ttl


class TestDatasetDescription(unittest.TestCase):
    source = examples_root / 'dd-pie.csv'

    def test_dd_pie_p(self):
        p = pipes.DatasetDescriptionFilePipeline(self.source, None, None)
        data = p.data
        # TODO test as subpipeline ?
