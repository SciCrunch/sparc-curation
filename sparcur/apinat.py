import rdflib
from pyontutils.core import OntGraph, OntId
from pyontutils.namespaces import rdf, rdfs, owl
import sparcur.schemas as sc

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
        if 'class' in blob:
            assert self.__class__.__name__ == blob['class']

    @property
    def cname(self):
        return self.__class__.__name__


apinscm = sc.ApiNATOMYSchema()
class Graph(Base):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        apinscm.validate(self.blob)

    @property
    def triples(self):
        context = rdflib.Namespace(f'https://apinatomy.org/uris/models/{self.id}/ids/')
        for cls in [Node, Link, Lyph, Tree, Group]:
            if cls.key in self.blob:
                # FIXME trees not in all blobs
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
    annotations = tuple()
    generics = tuple()
    objects = tuple()
    objects_multi = tuple()

    def triples(self):
        s = self.s
        yield s, rdf.type, owl.Class
        yield s, rdf.type, elements[f'{self.cname}']
        yield s, rdfs.label, rdflib.Literal(self.name)
        yield from self.triples_external()
        yield from self.triples_annotations()
        yield from self.triples_generics()
        yield from self.triples_objects()
        yield from self.triples_objects_multi()

    def triples_generics(self):
        for key in self.generics:
            # FIXME has + key.capitalize()?
            yield self.s, readable[key], readable[self.blob[key]]

    def triples_annotations(self):
        for key in self.annotations:
            if key in self.blob:
                value = self.blob[key]
                yield self.s, readable[key], rdflib.Literal(value)

    def triples_objects(self):
        for key in self.objects:
            if key in self.blob:
                value = self.blob[key]
                yield self.s, readable[key], self.context[value]

    def triples_objects_multi(self):
        for key in self.objects_multi:
            if key in self.blob:
                values = self.blob[key]
                for value in values:
                    yield self.s, readable[key], self.context[value]

    def triples_external(self):
        if 'externals' in self.blob:
            for external in self.blob['externals']:
                yield self.s, rdf.type, OntId(external).u


class Node(BaseElement):
    key = 'nodes'
    objects = tuple()
    objects_multi = 'sourceOf', 'targetOf'
    annotations = 'skipLabel', 'color'

    def triples(self):
        yield from super().triples()


class Lyph(BaseElement):
    key = 'lyphs'
    objects = 'layerIn', 'conveyedBy',
    objects_multi = 'inCoalescences',
    generics = 'topology',
    annotations = 'width', 'height', 'layerWidth'

    def triples(self):
        yield from super().triples()


class Link(BaseElement):
    key = 'links'


class Coalescence(BaseElement):
    key = 'coalescences'


class Border(BaseElement):
    # FIXME class is Link ?
    key = 'borders'


class Tree(BaseElement):
    key = 'trees'
    objects = 'root', 'lyphTemplate'
    objects_multi = 'housingLyphs',


class Group(BaseElement):
    key = 'groups'
    elements = Node, Link, Lyph, Coalescence  # Group  # ah class scope

    def triples(self):
        yield from super().triples()
        for element_class in self.elements:
            for value_local_id in self.blob[element_class.key]:
                c = element_class({'id':value_local_id}, context=self.context)
                yield self.s, readable.hasElement, c.s

Group.elements += (Group,)
