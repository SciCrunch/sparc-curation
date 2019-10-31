import json
import unittest
from pathlib import Path
from sparcur.apinat import *
import pytest

debug = False


class TestApiNATToRDF(unittest.TestCase):
    def test_1(self):
        with open((Path(__file__).parent / 'apinatomy/data/test_1_map.json'), 'rt') as f:
            m = json.load(f)
        with open((Path(__file__).parent / 'apinatomy/data/test_1_generated.json'), 'rt') as f:
            g = json.load(f)

        apin = Graph(m, g)
        rdfg = apin.graph()
        rtmap = Graph.fromRdf(rdfg).map
        assert list(rdfg)
        assert rtmap['resources']
        if debug:
            print(rdfg.ttl)
            print(rtmap)
            assert False

    def test_2(self):
        with open((Path(__file__).parent / 'apinatomy/data/test_2_map.json'), 'rt') as f:
            m = json.load(f)
        with open((Path(__file__).parent / 'apinatomy/data/test_2_generated.json'), 'rt') as f:
            g = json.load(f)

        apin = Graph(m, g)
        rdfg = apin.graph()
        rtmap = Graph.fromRdf(rdfg).map
        assert list(rdfg)
        assert rtmap['resources']
        if debug:
            print(rdfg.ttl)
            print(rtmap)
            assert False

    def test_3(self):
        with open((Path(__file__).parent / 'apinatomy/data/test_3_map.json'), 'rt') as f:
            m = json.load(f)
        with open((Path(__file__).parent / 'apinatomy/data/test_3_generated.json'), 'rt') as f:
            g = json.load(f)

        apin = Graph(m, g)
        rdfg = apin.graph()
        rtmap = Graph.fromRdf(rdfg).map
        assert list(rdfg)
        assert rtmap['resources']
        if debug:
            print(rdfg.ttl)
            print(rtmap)
            assert False

    def test_4(self):
        with open((Path(__file__).parent / 'apinatomy/data/test_4_map.json'), 'rt') as f:
            m = json.load(f)
        with open((Path(__file__).parent / 'apinatomy/data/test_4_generated.json'), 'rt') as f:
            g = json.load(f)

        apin = Graph(m, g)
        rdfg = apin.graph()
        rtmap = Graph.fromRdf(rdfg).map
        assert list(rdfg)
        assert rtmap['resources']
        if debug:
            print(rdfg.ttl)
            print(rtmap)
            assert False

    def test_5(self):
        with open((Path(__file__).parent / 'apinatomy/data/test_5_map.json'), 'rt') as f:
            m = json.load(f)
        # FIXME generated is broken at the moment
        with open((Path(__file__).parent / 'apinatomy/data/test_5_model.json'), 'rt') as f:
            g = json.load(f)

        apin = Graph(m, g)
        rdfg = apin.graph()
        rtmap = Graph.fromRdf(rdfg).map
        assert list(rdfg)
        assert rtmap['resources']
        if debug:
            print(rdfg.ttl)
            print(rtmap)
            assert False

    @pytest.mark.skip('not ready')
    def test_bolew(self):
        #with open((Path(__file__).parent / 'apinatomy/data/bolser-lewis-map.json'), 'rt') as f:
            #m = json.load(f)
        m = {'id':'null', 'resources':{}}
        with open((Path(__file__).parent / 'apinatomy/data/bolser-lewis-generated.json'), 'rt') as f:
            g = json.load(f)

        apin = Graph(m, g)
        rdfg = apin.graph()
        rtmap = Graph.fromRdf(rdfg).map
        if debug:
            print(rdfg.ttl)
            print(rtmap)
            assert False


class TestRDFToOWL2(unittest.TestCase):
    pass
