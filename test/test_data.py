import unittest
from pprint import pprint
from typing import Union, Dict, List, Tuple

import idlib
import rdflib
import pytest
from rdflib.plugins import sparql
from pyontutils.core import OntResIri, OntGraph
from pyontutils.namespaces import UBERON
from .common import skipif_no_net

Semantic = Union[rdflib.URIRef, rdflib.Literal, rdflib.BNode]


@skipif_no_net
class TestCurationExportTtl(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ori = OntResIri('https://cassava.ucsd.edu/sparc/exports/curation-export.ttl')
        cls.graph = cls.ori.graph
        cls.nsm = cls.graph.namespace_manager
        cls.spaql_templates = SparqlQueryTemplates(cls.nsm)

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
        subj = rdflib.URIRef('https://api.blackfynn.io/datasets/N:dataset:c2a014b8-2c15-4269-b10a-3345420e3d56/subjects/53')
        query = self.spaql_templates.dataset_subjects()
        res = list(self.graph.query(query, initBindings={'startsubj': subj}))
        self.pp(res)
        assert len(res) > 0

    def test_dataset_groups(self):
        #subj = rdflib.URIRef('https://api.blackfynn.io/datasets/N:dataset:c2a014b8-2c15-4269-b10a-3345420e3d56/subjects/53')
        subj = rdflib.URIRef('https://api.blackfynn.io/datasets/N:dataset:3a7ccb46-4320-4409-b359-7f4a7027bb9c/samples/104_sample4')
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

        self.pp(res, unpack=False)
        assert len(res) > 0


class SparqlQueryTemplates:
    """ Creates SPARQL query templates. """

    def __init__(self, nsm=None):
        self.nsm = nsm if nsm else OntGraph().namespace_manager
        self.prefixes = dict(self.nsm)

    def sparql_iri(self, iri: Union[rdflib.URIRef, str]) -> str:
        """ Converts IRIs and curies to a usable format for SPARQL queries. """
        if iri.startswith('http') or isinstance(iri, rdflib.URIRef):
            return '<'+str(iri)+'>'
        return iri

    def dataset_about(self):
        # FIXME this will return any resource matching isAbout:
        query = """
            SELECT ?dataset
            WHERE {
                ?dataset rdf:type sparc:Resource .
                ?dataset isAbout: ?about .
            }
        """
        return sparql.prepareQuery(query, initNs=self.prefixes)

    def dataset_subjects(self) -> str:
        """ Get all subject groups and dataset associated with subject input.

        :returns: list of tuples containing: subject, subjects group, and subjects dataset.
        """
        query = """
            SELECT ?dataset ?subj
            WHERE {
                ?startsubj TEMP:hasDerivedInformationAsParticipant ?dataset .
                ?subj  TEMP:hasDerivedInformationAsParticipant ?dataset .
            }
        """
        return sparql.prepareQuery(query, initNs=self.prefixes)

    def dataset_groups(self) -> str:
        """ Get all subject groups and dataset associated with subject input.

        :returns: list of tuples containing: subject, subjects group, and subjects dataset.
        """
        query = """
            SELECT ?dataset ?group ?subj
            WHERE {
                ?startsubj TEMP:hasDerivedInformationAsParticipant ?dataset .
                ?subj  TEMP:hasDerivedInformationAsParticipant ?dataset .
                ?subj  TEMP:hasAssignedGroup ?group .
            }
        """
        return sparql.prepareQuery(query, initNs=self.prefixes)

    def dataset_bundle(self) -> str:
        """ Get all related datasets of subject.

        :returns: list of tuples containing: subject & subjects shared dataset.
        """
        query = """
            SELECT ?dataset
            WHERE {
                ?startdataset TEMP:collectionTitle ?string .
                ?dataset  TEMP:collectionTitle ?string .
            }
        """
        return sparql.prepareQuery(query, initNs=self.prefixes)

    def dataset_subject_species(self):
        # FIXME how to correctly init bindings to multiple values ...
        query = """
            SELECT DISTINCT ?dataset
            WHERE {
                VALUES ?species { "human" "homo sapiens" } .
                ?dataset TEMP:isAboutParticipant ?subject .
                ?subject sparc:animalSubjectIsOfSpecies ?species .
            }
        """
        return sparql.prepareQuery(query, initNs=self.prefixes)

    def award_affiliations(self):
        query = """
            SELECT DISTINCT ?award ?affiliation
            WHERE {
                ?dataset TEMP:hasAwardNumber ?award .
                ?contributor TEMP:contributorTo ?dataset .
                ?contributor TEMP:hasAffiliation ?affiliation .
            }
        """
        return sparql.prepareQuery(query, initNs=self.prefixes)
