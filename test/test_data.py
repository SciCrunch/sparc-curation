from typing import Union, Dict, List, Tuple
import unittest

import rdflib
from rdflib.plugins import sparql
import pytest

from pyontutils.core import OntResIri

Semantic = Union[rdflib.URIRef, rdflib.Literal, rdflib.BNode]


class TestCurationExportTtl(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ori = OntResIri('https://cassava.ucsd.edu/sparc/exports/curation-export.ttl')
        cls.graph = cls.ori.graph
        cls.spaql_templates = SparqlQueryTemplates(cls.graph)

    def test_dataset_subjs(self):
        """ sparql queries here """
        subj = rdflib.URIRef('https://api.blackfynn.io/datasets/N:dataset:c2a014b8-2c15-4269-b10a-3345420e3d56/subjects/53')
        query = self.spaql_templates.dataset_subjs()
        assert len(list(self.graph.query(query, initBindings={'startsubj': subj}))) > 0

    def test_dataset_groups(self):
        """ sparql queries here """
        subj = rdflib.URIRef('https://api.blackfynn.io/datasets/N:dataset:c2a014b8-2c15-4269-b10a-3345420e3d56/subjects/53')
        query = self.spaql_templates.dataset_groups()
        assert len(list(self.graph.query(query, initBindings={'startsubj': subj}))) > 0

    def test_related_datasets(self):
        subj = rdflib.util.from_n3('dataset:bec4d335-9377-4863-9017-ecd01170f354', nsm=self.graph)
        query = self.spaql_templates.related_datasets()
        assert len(list(self.graph.query(query, initBindings={'startdataset': subj}))) > 0


class SparqlQueryTemplates:
    """ Creates SPARQL query templates. """

    def __init__(self, nsm=None):
        self.nsm = nsm if nsm else rdflib.Graph().namespace_manager
        self.prefixes = {p:ns for p, ns in self.nsm.namespaces() if p}

    def sparql_iri(self, iri: Union[rdflib.URIRef, str]) -> str:
        """ Converts IRIs and curies to a usable format for SPARQL queries. """
        if iri.startswith('http') or isinstance(iri, rdflib.URIRef):
            return '<'+str(iri)+'>'
        return iri

    def dataset_subjs(self) -> str:
        """ Get all subject groups and dataset associated with subject input.

        :returns: list of tuples containing: subject, subjects group, and subjects dataset.
        """
        query = """
            SELECT ?subj ?dataset
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
            SELECT ?subj ?group ?dataset
            WHERE {
                ?startsubj TEMP:hasDerivedInformationAsParticipant ?dataset .
                ?subj  TEMP:hasDerivedInformationAsParticipant ?dataset .
                ?subj  TEMP:hasAssignedGroup ?group .
            }
        """
        return sparql.prepareQuery(query, initNs=self.prefixes)

    def related_datasets(self) -> str:
        """ Get all related datasets of subject.

        :returns: list of tuples containing: subject & subjects shared dataset.
        """
        query = """
            SELECT ?subj ?dataset
            WHERE {
                ?startdataset TEMP:collectionTitle ?string .
                ?subj  TEMP:collectionTitle ?string .
            }
        """
        return sparql.prepareQuery(query, initNs=self.prefixes)
