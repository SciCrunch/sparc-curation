import rdflib
from pyontutils.core import OntGraph, OntId
from pyontutils.namespaces import rdf, rdfs, owl
import sparcur.schemas as sc
from sparcur.core import adops
from sparcur.utils import log, logd

elements = rdflib.Namespace('https://apinatomy.org/uris/elements/')
readable = rdflib.Namespace('https://apinatomy.org/uris/readable/')



class NoIdError(Exception):
    """ blob has no id """


apinscm = sc.ApiNATOMYSchema()


def make_classes(schema):
    types = {}

    def ref_to_list(ref):
        _jpath = ref.split('/')
        if _jpath[0] != '#':
            raise ValueError(ref)
        else:
            jpath = _jpath[1:]

        return jpath

    def deref(ref):
        return adops.get(schema, ref_to_list(ref))

    def allOf(obj):
        for o in obj['allOf']:
            if '$ref' in o:
                ref = o['$ref']
                if ref in types:
                    yield types[ref]
                else:
                    jpath = ref_to_list(ref)
                    no = adops.get(schema, jpath)
                    yield top(jpath[-1], no)
            else:
                log.debug(f'{obj}')

    def properties(obj):
        props = obj['properties']
        out = {}
        for name, vobj in props.items():
            @property
            def f(self, n=name):
                return self.blob[n]

            out[name] = f

        return out

    def top(cname, obj):
        deref, allOf, ref_to_list, schema, types  # python is dumb
        type_ = None
        if 'type' in obj:
            type_ = obj['type']
            parents = (Base,)

        elif 'allOf' in v:
            parents = tuple(allOf(obj))
            for c in parents:
                if hasattr(c, 'type'):
                    type_ = c.type

        if type_ is None:
            raise TypeError('wat')

        cd = {'type': type_,
              '_schema': obj,
        }

        if type_ == 'object':
            props = properties(obj)
            for n, f in props.items():
                cd[n] = f

        return type(cname, parents, cd)

    cs = []
    d = schema['definitions']
    for k, v in d.items():
        c = top(k, v)
        ref = f'#/definitions/{k}'
        types[ref] = c
        cs.append(c)

    #breakpoint()
    return cs


class Base:
    def __init__(self, blob, context=None):
        self.blob = blob
        self.context = context
        try:
            self.id = blob['id']
        except KeyError as e:
            raise NoIdError(f'id not in {blob}') from e
        except AttributeError:
            pass  # new impl uses properties to access the blob

        if context is not None:
            self.s = context[self.id]
        else:
            self.s = None

        try:
            self.name = blob['name'] if 'name' in blob else self.id
        except AttributeError:
            pass  # new impl uses properties to access the blob

        if 'class' in blob:
            assert self.__class__.__name__ == blob['class']

    @property
    def cname(self):
        return self.__class__.__name__


class Graph(Base):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        apinscm.validate(self.blob)

    @property
    def triples(self):
        context = rdflib.Namespace(f'https://apinatomy.org/uris/models/{self.id}/ids/')
        for cls in [Node, Link, Lyph, Tree, Group, Material]:
            if cls.key in self.blob:
                # FIXME trees not in all blobs
                for blob in self.blob[cls.key]:
                    try:
                        yield from cls(blob, context).triples()
                    except NoIdError as e:
                        logd.exception(e)

    def populate(self, graph):
        [graph.add(t) for t in self.triples]

    def graph(self):
        g = OntGraph()
        self.populate(g)
        g.bind('readable', readable)  # FIXME populate from store
        g.bind('elements', elements)
        return g


class BaseElement(Base):
    key = None
    annotations = tuple()
    generics = tuple()
    objects = tuple()
    objects_multi = tuple()

    def triples(self):
        s = self.s
        yield s, rdf.type, owl.NamedIndividual
        yield s, rdf.type, elements[f'{self.cname}']
        yield s, rdfs.label, rdflib.Literal(self.name)
        yield from self.triples_external()
        yield from self.triples_annotations()
        yield from self.triples_generics()
        yield from self.triples_objects()
        yield from self.triples_objects_multi()

    def triples_annotations(self):
        for key in self.annotations:
            if key in self.blob:
                value = self.blob[key]
                yield self.s, readable[key], rdflib.Literal(value)

    def triples_generics(self):
        for key in self.generics:
            # FIXME has + key.capitalize()?
            if key in self.blob:
                value = self.blob[key]
                yield self.s, readable[key], readable[value]
            else:
                log.warning(f'{key} not in {self.blob}')

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
    generics = 'conveyingType',


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
            if element_class.key in self.blob:
                for value_local_id in self.blob[element_class.key]:
                    c = element_class({'id':value_local_id}, context=self.context)
                    yield self.s, readable.hasElement, c.s
            else:
                log.warning(f'{element_class.key} not in {self.blob}')


class Material(BaseElement):
    key = 'materials'
    objects_multi = 'materials', 'inMaterials'


Group.elements += (Group,)


hrm = make_classes(apinscm.schema)
