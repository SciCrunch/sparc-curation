""" test the actual output of the curation export pipeline """
import unittest
from pprint import pprint
import idlib
import rdflib
import pytest
from pyontutils.core import OntResIri, OntGraph, OntResPath
from pyontutils.namespaces import UBERON, NIFSTD, asp, TEMP, TEMPRAW
from sparcur.paths import Path
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
        cls._q_protocol_aspects =  cls.spaql_templates.protocol_aspects()

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


@skipif_no_net
class TestProtcurTtl(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        #cls.orp = OntResPath()
        #cls.graph = cls.orp.graph
        path = Path('/home/tom/.local/share/sparcur/export/protcur/LATEST/protcur.ttl')
        path = Path('/home/tom/git/NIF-Ontology/ttl/protcur.ttl')
        cls.graph = OntGraph(path=path).parse()  # fixme the heck
        cls.nsm = cls.graph.namespace_manager
        cls.spaql_templates = SparqlQueries(cls.nsm)
        cls._q_protocol_aspects =  cls.spaql_templates.protocol_aspects()
        cls._q_protocol_inputs =  cls.spaql_templates.protocol_inputs()

    def test_protcur_techniques(self):
        query = self.spaql_templates.protocol_techniques()
        #res = list(self.graph.query(query))
        res = list(self.graph.query(
            query,
            initBindings={
                'technique':
                #rdflib.Literal('confocal microscopy technique')
                #rdflib.Literal('microscopy')
                rdflib.Literal('Microscopy')
                #rdflib.Literal('microscopy technique')
            }))
        print(res)
        assert len(res) > 0

    def _query_protocol_aspect(self, aspect=None):
        query = self._q_protocol_aspects
        bindings = {'aspect': rdflib.Literal(aspect)} if aspect else {}
        res = list(self.graph.query(query, initBindings=bindings))
        return res

    def test_protocol_aspect_terms(self):
        aspects = [
            #'magnification',
            #'temperature',
            #'fold',
                   ]

        results = []
        for aspect in aspects:
            res = self._query_protocol_aspect(aspect)
            results.append(res)

        pids = [[pid for pid, lit in sorted(rs)] for rs in results]
        [print(p) for p in pids]
        print(set().intersection(*pids))
        _ = [print(r) for rs in results for r in rs]

    def _query_protocol_input(self, inp):
        query = self._q_protocol_inputs
        res = list(self.graph.query(query, initBindings={'input': inp}))
        return res

    def test_protcur_aspects(self):
        res = self._query_protocol_aspect()
        results = [res]

        pids = [[pid for pid, lit in rs] for rs in results]
        [print(p) for p in pids]
        print(set().intersection(*pids))
        _ = [print(r) for rs in results for r in rs]

    def test_stain_hack(self):
        stain = sorted(set([s for s, o in self.graph[:TEMPRAW.protocolInvolvesAction:]
                            if 'stain' in o.lower()]))
        [print(s) for s in stain]

    def test_protcur_aspects_mic(self):
        aspects = [
            #NIFSTD.DB01221,  # ketamine
            #'NIFSTD:DB01221',  # ketamine  # doesn't work
            asp.magnification,
            asp.contrast,
                   ]

        results = []
        for aspect in aspects:
            res = self._query_protocol_aspect(aspect)
            results.append(res)

        pids = [[pid for pid, lit in sorted(rs)] for rs in results]
        [print(p) for p in pids]
        print(set().intersection(*pids))
        _ = [print(r) for rs in results for r in rs]

    def test_protcur_aspects_ana(self):
        aspects = [
            #NIFSTD.DB01221,  # ketamine
            #'NIFSTD:DB01221',  # ketamine  # doesn't work
            asp.anaesthetized
                   ]

        results = []
        for aspect in aspects:
            res = self._query_protocol_aspect(aspect)
            results.append(res)

        pids = [[pid for pid, lit in sorted(rs)] for rs in results]
        [print(p) for p in pids]
        print(set().intersection(*pids))
        _ = [print(r) for rs in results for r in rs]

    def test_protcur_inputs(self):
        inputs = [
            NIFSTD.DB01221,  # ketamine
            #'NIFSTD:DB01221',  # ketamine  # doesn't work
                   ]

        results = []
        for inp in inputs:
            res = self._query_protocol_input(inp)
            results.append(res)

        pids = [[pid for pid, ast, lit in sorted(rs)] for rs in results]
        [print(p) for p in pids]
        print(set().intersection(*pids))
        _ = [print(r) for rs in results for r in rs]

