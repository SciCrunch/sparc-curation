import json
import unittest
from pathlib import Path
from sparcur.apinat import *


class TestApiNATToRDF(unittest.TestCase):
    def test_1(self):
        with open((Path(__file__).parent / 'apinatomy/data/test_1_model.json'), 'rt') as f:
            j = json.load(f)

        apin = ApiNATOMY(j)
        print(apin.graph().ttl)
        assert False


class TestRDFToOWL2(unittest.TestCase):
    pass
