import unittest
from pathlib import Path
import pytest
from sparcur import pipelines as pipes
from .common import examples_root


class TestApiNAT(unittest.TestCase):

    source = Path(__file__).parent / 'apinatomy/data'

    def test_load(self):
        m = pipes.ApiNATOMY(Path(self.source, 'keast-spinal-model.json'))
        # FIXME I think only the model conforms to the schema ?
        #g = pipes.ApiNATOMY(Path(self.source, 'apinatomy-generated.json'))
        #rm = pipes.ApiNATOMY(Path(self.source, 'apinatomy-resourceMap.json'))
        m.data.keys()
        #asdf = m.data.keys(), g.data.keys(), rm.data.keys()

    @pytest.mark.skip('hardcoded assumptions mean this does not work yet')
    def test_export_model(self):
        m = pipes.ApiNATOMY(Path(self.source, 'keast-spinal-model.json'))
        # FIXME need a way to combine this that doesn't require
        # the user to know how to compose these, just send a message
        # to one of them they should be able to build the other from
        # the information at hand
        r = pipes.ApiNATOMY_rdf(m)
        r.data

    def test_export_rm(self):
        rm = pipes.ApiNATOMY(Path(self.source, 'keast-spinal-map.json'))
        r = pipes.ApiNATOMY_rdf(rm)  # FIXME ... should be able to pass the pipeline
        r.data
        #breakpoint()


class TestDatasetDescription(unittest.TestCase):
    source = examples_root / 'dd-pie.csv'

    def test_dd_pie_p(self):
        p = pipes.DatasetDescriptionFilePipeline(self.source, None, None)
        data = p.data
        # TODO test as subpipeline ?
