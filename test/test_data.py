import unittest
from pprint import pprint
import idlib
import rdflib
import pytest
from pyontutils.core import OntResIri, OntGraph
from pyontutils.namespaces import UBERON
from sparcur.reports import SparqlQueries
from .common import skipif_no_net


@skipif_no_net
class TestCurationExportTtl(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.ori = OntResIri('https://cassava.ucsd.edu/sparc/exports/curation-export.ttl')
        cls.graph = cls.ori.graph
        cls.nsm = cls.graph.namespace_manager
        cls.spaql_templates = SparqlQueries(cls.nsm)

    @classmethod
    def pp(cls, res, unpack=False):
        print()
        print(len(res))
        if unpack:
            pprint(sorted([cls.nsm._qhrm(e)
                           if isinstance(e, rdflib.URIRef) else
                           e.toPython()
                           for r in res
                           for e in r]))
        else:
            pprint(sorted([tuple(cls.nsm._qhrm(e)
                                 if isinstance(e, rdflib.URIRef) else e.toPython()
                                 for e in r)
                           for r in res]))

    def test_dataset_about_heart(self):
        subj = UBERON['0000948']
        query = self.spaql_templates.dataset_about()
        res = list(self.graph.query(query, initBindings={'about': subj}))
        self.pp(res, unpack=True)
        assert len(res) > 0

    def test_dataset_subjects(self):
        subj = rdflib.URIRef('https://api.blackfynn.io/datasets/'
                             'N:dataset:c2a014b8-2c15-4269-b10a-3345420e3d56/subjects/53')
        query = self.spaql_templates.dataset_subjects()
        res = list(self.graph.query(query, initBindings={'startsubj': subj}))
        self.pp(res)
        assert len(res) > 0

    def test_dataset_groups(self):
        #subj = rdflib.URIRef('https://api.blackfynn.io/datasets/'
                             #'N:dataset:c2a014b8-2c15-4269-b10a-3345420e3d56/subjects/53')
        subj = rdflib.URIRef('https://api.blackfynn.io/datasets/'
                             'N:dataset:3a7ccb46-4320-4409-b359-7f4a7027bb9c/samples/104_sample4')
        query = self.spaql_templates.dataset_groups()
        res = list(self.graph.query(query, initBindings={'startsubj': subj}))
        self.pp(res)
        assert len(res) > 0

    def test_dataset_bundle(self):
        subj = rdflib.util.from_n3('dataset:bec4d335-9377-4863-9017-ecd01170f354', nsm=self.nsm)
        query = self.spaql_templates.dataset_bundle()
        res = list(self.graph.query(query, initBindings={'startdataset': subj}))
        self.pp(res, unpack=True)
        assert len(res) > 0

    def test_dataset_subject_species(self):
        query = self.spaql_templates.dataset_subject_species()
        res = list(self.graph.query(query))
        self.pp(res, unpack=True)
        assert len(res) > 0

    def test_award_affil(self):
        query = self.spaql_templates.award_affiliations()
        res = list(self.graph.query(query))
        out = {}
        for award, affil in res:
            award = self.nsm._qhrm(award)
            if isinstance(affil, rdflib.URIRef):
                ror = idlib.Ror(affil)
                affil = f'{ror.identifier.curie} ({ror.label})'
            else:
                continue  # skip these for now
                affil = affil.toPython()

            if award not in out:
                out[award] = []

            out[award].append(affil)

        for k, vs in sorted(out.items()):
            vs.sort()
            print(k)
            [print('\t', v) for v in vs]

        print('unique awards:', len(out))
        print('unique affils:', len(set([v for vs in out.values() for v in vs])))
        self.pp(res, unpack=False)
        assert len(res) > 0

    def test_protocol_techniques(self):
        query = self.spaql_templates.protocol_techniques()
        res = list(self.graph.query(query))
        print(res)
        assert len(res) > 0

    def test_protocol_aspects(self):
        query = self.spaql_templates.protocol_aspects()
        res = list(self.graph.query(query))
        print(res)
        assert len(res) > 0
