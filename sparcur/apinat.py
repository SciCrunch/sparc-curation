import rdflib
from ontquery.utils import mimicArgs
from pyontutils.core import OntGraph, OntId, OntTerm
from pyontutils.namespaces import rdf, rdfs, owl, OntCuries
import sparcur.schemas as sc
from sparcur.core import adops
from sparcur.utils import log, logd
from ttlser import CustomTurtleSerializer


elements = rdflib.Namespace('https://apinatomy.org/uris/elements/')
readable = rdflib.Namespace('https://apinatomy.org/uris/readable/')

# add apinatomy:Graph to ttlser topClasses
tc = CustomTurtleSerializer.topClasses
if readable.Graph not in tc:
    sec = CustomTurtleSerializer.SECTIONS
    CustomTurtleSerializer.topClasses = [readable.Graph] + tc
    CustomTurtleSerializer.SECTIONS = ('',) + sec

# add apinatomy:Graph as a header section marker
OntGraph.metadata_type_markers.append(readable.Graph)


OntCuries({'apinatomy': str(readable),
           'elements': str(elements),  # FIXME guranteed name collisions ...
           # also just read this from the embedded local conventions
})


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

    @classmethod
    def fromRdf(cls, uri, graph, context=None):
        _, id = uri.rsplit('/', 1)
        blob = {'id': id}
        for p, o in graph[uri]:
            if p == rdf.type:
                if o != owl.NamedIndividual:
                    key = 'class'
                    _, value = o.rsplit('/', 1)
                else:
                    continue  # TODO s rdf:type apinatomy:External ??
            else:
                if p == rdfs.label:
                    key = 'name'
                else:
                    _, key = p.rsplit('/', 1)

                if isinstance(o, rdflib.Literal):
                    value = o.toPython()
                elif isinstance(o, rdflib.URIRef):
                    oid = OntId(o)
                    if oid.prefix == 'local':
                        value = oid.suffix
                    elif oid.prefix == 'apinatomy':  # FIXME hrm?
                        value = oid.suffix
                    else:
                        value = oid.curie  # FIXME external is tricky
                        log.warning(f'{oid!r}')
                else:
                    raise NotImplementedError(f'{o}')
                
            if key in cls.objects_multi:
                if key in blob:
                    blob[key].append(value)
                else:
                    blob[key] = [value]

            else:
                blob[key] = value
            
        return cls(blob, context)

    def __init__(self, blob, context=None):
        self.blob = blob
        self.context = context
        try:
            self.id = blob['id'].replace(' ', '-')  #FIXME
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

    @classmethod
    def fromRdf(cls, graph):
        iri = graph.boundIdentifier
        context = rdflib.Namespace(iri + '/ids/')
        # TODO removing things from the trie is not implemented ...
        #d = OntCuries._dict
        #d.pop('local', None)
        #d['local'] = str(context)
        #OntCuries.reset()
        OntCuries({'local': str(context)})
        _, id = iri.rsplit('/', 1)
        resources = {}
        for s in graph[:rdf.type:owl.NamedIndividual]:
            for element in graph[s:rdf.type]:
                if element != owl.NamedIndividual:
                    _, class_ = element.rsplit('/', 1)
                    resource = getattr(cls, class_).fromRdf(s, graph, context)
                    # FIXME we should really keep the internal representation
                    # around instead of just throwing it away
                    resources[resource.id] = resource.blob

        for s in graph[:rdf.type:owl.Class]:
            # FIXME s rdf:type elements:External ??
            resource = External.fromRdf(s, graph, context)
            resources[resource.id] = resource.blob

        map = {'id': id,
               'resources': resources}
        
        return cls(map, {})

    def __init__(self, map, blob):
        self.map = map
        self.resources = map['resources']
        self.prefixes = {}  # TODO curie mapping
        self.id = self.map['id'].replace(' ', '-')  # FIXME
        self.blob = blob
        #apinscm.validate(self.blob)  # TODO

    @property
    def context(self):
        return rdflib.Namespace(f'{self.iri}/ids/')

    @property
    def triples(self):
        self.iri = rdflib.URIRef(f'https://apinatomy.org/uris/models/{self.id}')
        yield self.iri, rdf.type, readable.Graph
        for id, blob in self.resources.items():
            if 'class' not in blob:
                logd.warning(f'no class in\n{blob!r}')
                continue
            elif blob['class'] == 'Graph':
                log.warning('Graph is in resources itself')
                continue

            yield from getattr(self, blob['class'])(blob, self.context).triples()

    @property
    def triples_generated(self):
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
        #[graph.add(t) for t in self.triples]
        for t in self.triples:
            s, p, o = t
            if s is None:
                raise BaseException(f'{t}')

            graph.add(t)

    def graph(self):
        g = OntGraph()
        OntCuries.populate(g)
        self.populate(g)
        g.bind('local', self.context)
        g.bind('apinatomy', readable)  # FIXME populate from store
        g.bind('elements', elements)
        return g


class BaseElement(Base):
    key = None
    annotations = tuple()
    generics = tuple()
    objects = tuple()
    objects_multi = tuple()

    def triples(self):
        yield from self.triples_class()
        yield from self.triples_external()
        yield from self.triples_annotations()
        yield from self.triples_generics()
        yield from self.triples_objects()
        yield from self.triples_objects_multi()

    def triples_class(self):
        yield self.s, rdf.type, owl.NamedIndividual
        yield self.s, rdf.type, elements[f'{self.cname}']
        yield self.s, rdfs.label, rdflib.Literal(self.name)

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
                value = value.replace(' ', '-')  # FIXME require no spaces in internal ids
                yield self.s, readable[key], self.context[value]

    def triples_objects_multi(self):
        for key in self.objects_multi:
            if key in self.blob:
                values = self.blob[key]
                for value in values:
                    value = value.replace(' ', '-')  # FIXME require no spaces in internal ids
                    yield self.s, readable[key], self.context[value]

    def triples_external(self):
        if 'externals' in self.blob:
            for external in self.blob['externals']:
                yield self.s, rdf.type, OntId(external).URIRef


class Node(BaseElement):
    key = 'nodes'
    annotations = 'skipLabel', 'color'
    objects = tuple()
    objects_multi = 'sourceOf', 'targetOf'

    def triples(self):
        yield from super().triples()

Graph.Node = Node
class Lyph(BaseElement):
    key = 'lyphs'
    generics = 'topology',
    annotations = 'width', 'height', 'layerWidth', 'internalLyphColumns', 'isTemplate'
    objects = 'layerIn', 'conveyedBy', 'border'
    objects_multi = 'inCoalescences', 'subtypes', 'layers'

    def triples(self):
        yield from super().triples()
Graph.Lyph = Lyph


class Link(BaseElement):
    key = 'links'
    generics = 'conveyingType',
    objects = 'source', 'target', 'conveyingLyph'
    objects_multi = 'conveyingMaterials',


Graph.Link = Link
class Coalescence(BaseElement):
    key = 'coalescences'
    generics = 'topology',
    annotations = 'generated',
    objects = 'generatedFrom',
    objects_multi = 'lyphs',


Graph.Coalescence = Coalescence
class Border(BaseElement):
    # FIXME class is Link ?
    key = 'borders'


Graph.Border = Border
class Tree(BaseElement):
    key = 'trees'
    objects = 'root', 'lyphTemplate'
    objects_multi = 'housingLyphs',


Graph.Tree = Tree
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


Group.elements += (Group,)
Graph.Group = Group
class Material(BaseElement):
    key = 'materials'
    objects_multi = 'materials', 'inMaterials'


class fake:
    """ filler """
    label = 'sigh'


Graph.Material = Material
class External(BaseElement):
    externals = 'id',
    annotations = 'generated', 'uri', 'type',  # FIXME should be classes
    objects_multi = 'externalTo',

    @classmethod
    def fromRdf(cls, uri, graph, context=None):
        oid = OntId(uri)
        id = oid.curie
        blob = {'id': id}
        for p, o in graph[uri]:
            if p == rdf.type:
                key = 'class'
                value = 'External'
            else:
                if p == rdfs.label:
                    key = 'name'
                else:
                    _, key = p.rsplit('/', 1)

                if isinstance(o, rdflib.Literal):
                    value = o.toPython()
                elif isinstance(o, rdflib.URIRef):
                    oid = OntId(o)
                    if oid.prefix == 'local':
                        value = oid.suffix
                    elif oid.prefix == 'apinatomy':  # FIXME hrm?
                        value = oid.suffix
                    else:
                        value = oid.curie  # FIXME external is tricky
                        log.warning(f'{oid!r}')

            if key in cls.objects_multi:
                if key in blob:
                    blob[key].append(value)
                else:
                    blob[key] = [value]

            else:
                blob[key] = value
            
        return cls(blob, context)


    @mimicArgs(BaseElement.__init__)
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # PLEASE DO NOT PUT PMIDs as external ids!!!
        # FIXME idlib PMID(thing) urg the regex state machine is so simple ;_;
        if self.id.startswith('PMID:'):
            log.warning('PMIDs should never be External IDs!')
            self._term = fake
            return

        self._term = OntTerm(self.id)
        self.s = self._term.URIRef

    def triples_class(self):
        yield self.s, rdf.type, owl.Class
        yield self.s, rdfs.label, rdflib.Literal(self._term.label)
        # TODO triples simple?


Graph.External = External
class Channel(BaseElement):
    pass


Graph.Channel = Channel
class Chain(BaseElement):
    objects = 'start', 'end'
    objects_multi = 'conveyingLyphs',
    annotations = 'length',


Graph.Chain = Chain


hrm = make_classes(apinscm.schema)
