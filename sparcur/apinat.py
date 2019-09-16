import rdflib
from pyontutils.core import OntGraph
from pyontutils.namespaces import rdf, rdfs, owl

elements = rdflib.Namespace('https://apinatomy.org/uris/elements/')
readable = rdflib.Namespace('https://apinatomy.org/uris/readable/')

class Base:
    def __init__(self, blob, context=None):
        self.blob = blob
        self.context = context
        self.id = blob['id']
        if context is not None:
            self.s = context[self.id]
        else:
            self.s = None
        self.name = blob['name'] if 'name' in blob else self.id
        self.Class = blob['class']


class ApiNATOMY(Base):
    @property
    def triples(self):
        context = rdflib.Namespace(f'https://apinatomy.org/uris/models/{self.id}/ids/')
        for cls in [Node, Link, Lyph]:
            for blob in self.blob[cls.key]:
                yield from cls(blob, context).triples()

    def populate(self, graph):
        [graph.add(t) for t in self.triples]

    def graph(self):
        g = OntGraph()
        self.populate(g)
        return g

class BaseElement(Base):
    key = None
    def triples(self):
        s = self.s
        yield s, rdf.type, owl.Class
        yield s, rdf.type, elements[f'{self.Class}']
        yield s, rdfs.label, rdflib.Literal(self.name)


class Node(BaseElement):
    key = 'nodes'

    def triples(self):
        yield from super().triples()
        if 'sourceOf' in self.blob:
            for so in self.blob['sourceOf']:
                yield self.s, readable.sourceOf, self.context[so]

        if 'targetOf' in self.blob:
            for to in self.blob['targetOf']:
                yield self.s, readable.targetOf, self.context[to]


class Lyph(BaseElement):
    key = 'lyphs'
    fields = 'layerIn',
    def triples(self):
        yield from super().triples()
        for field in self.fields:
            if field in self.blob:
                yield self.s, readable[field], self.context[self.blob[field]]

class Link(BaseElement):
    key = 'links'
