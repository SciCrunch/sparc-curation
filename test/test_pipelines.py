import unittest
from pathlib import Path
from sparcur import pipelines as pipes


class TestApiNAT(unittest.TestCase):

    source = Path('~/ni/sparc/apinat/sources/').expanduser()  # FIXME config probably

    def test_load(self):
        m = pipes.ApiNATOMY(Path(self.source, 'apinatomy-model.json'))
        g = pipes.ApiNATOMY(Path(self.source, 'apinatomy-generated.json'))
        rm = pipes.ApiNATOMY(Path(self.source, 'apinatomy-resourceMap.json'))
        asdf = m.data.keys(), g.data.keys(), rm.data.keys()
