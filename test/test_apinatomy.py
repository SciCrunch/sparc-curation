import json
import unittest
from pathlib import Path
from sparcur.apinat import *


class TestApiNATToRDF(unittest.TestCase):
    def test_1(self):
        with open((Path(__file__).parent / 'apinatomy/data/test_1_generated.json'), 'rt') as f:
            j = json.load(f)

        apin = Graph(j)
        print(apin.graph().ttl)
        assert False

    def test_2(self):
        with open((Path(__file__).parent / 'apinatomy/data/test_2_generated.json'), 'rt') as f:
            j = json.load(f)

        apin = Graph(j)
        print(apin.graph().ttl)
        assert False

    def test_3(self):
        with open((Path(__file__).parent / 'apinatomy/data/test_3_generated.json'), 'rt') as f:
            j = json.load(f)

        apin = Graph(j)
        print(apin.graph().ttl)
        assert False

    def test_bolew(self):
        with open((Path(__file__).parent / 'apinatomy/data/bolser-lewis-generated.json'), 'rt') as f:
            j = json.load(f)

        apin = Graph(j)
        assert False


class TestRDFToOWL2(unittest.TestCase):
    pass
