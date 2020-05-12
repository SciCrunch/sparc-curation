from types import MappingProxyType
import rdflib
from ontquery.utils import mimicArgs
from pyontutils.core import OntGraph
from pyontutils.utils import Async, deferred
from pyontutils.namespaces import rdf, rdfs, owl, OntCuries
from pyontutils import combinators as cmb
import sparcur.schemas as sc
from sparcur.core import adops, OntId, OntTerm
from sparcur.utils import log, logd
from ttlser import CustomTurtleSerializer


elements = rdflib.Namespace('https://apinatomy.org/uris/elements/')
readable = rdflib.Namespace('https://apinatomy.org/uris/readable/')
ordered = rdflib.Namespace('https://apinatomy.org/uris/readable/ordered/')

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
           'ordered': str(ordered),
           'PMID': 'https://www.ncbi.nlm.nih.gov/pubmed/',
           # also just read this from the embedded local conventions
})


class NoIdError(Exception):
    """ blob has no id """


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
        if not v:
            log.warning(f'empty definition for {k}')
            continue

        c = top(k, v)
        ref = f'#/definitions/{k}'
        types[ref] = c
        cs.append(c)

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
                elif isinstance(o, rdflib.BNode):
                    raise NotImplementedError(f'a bit more complex ...')
                else:
                    raise NotImplementedError(f'{o}')
                
            if key in cls.objects_ordered:  # ordered representation takes priority
                raise NotImplementedError('TODO this is quite a bit more complex')
                if key in blob:
                    blob[key].append(value)
                else:
                    blob[key] = [value]

            elif key in cls.objects_multi:
                if key in blob:
                    blob[key].append(value)
                else:
                    blob[key] = [value]

            else:
                blob[key] = value

        return cls(blob, context)

    def __init__(self, blob, context=None, label_suffix=''):
        self.blob = blob
        self.context = context
        self.label_suffix = label_suffix
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

        blob = {'id': id}
        resources = {id: blob}
        map = {'id': id,
               'resources': resources}

        for p, o in graph[iri:]:
            if p == rdf.type:
                blob['class'] = o.toPython()

            else:
                _, key = p.rsplit('/', 1)
                blob[key] = o.toPython()

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

        return cls(map)

    def __init__(self, map):
        self.map = map
        self.resources = map['resources']  # FIXME resources.pop('waitingList') ??
        self.prefixes = {}  # TODO curie mapping
        self.id = self.map['id'].replace(' ', '-')  # FIXME

        self.blob = self.resources[self.map['id']]
        # FIXME should we require name and abbrev and make missing a fatal error?
        self.name = self.blob['name'] if 'name' in self.blob else None
        self.abbreviation = self.blob['abbreviation'] if 'abbreviation' in self.blob else None
        self.label_suffix = f' ({self.abbreviation})' if self.abbreviation else ''
        #apinscm.validate(self.blob)  # TODO

    @property
    def context(self):
        return rdflib.Namespace(f'{self.iri}/ids/')

    @property
    def triples(self):
        self.iri = rdflib.URIRef(f'https://apinatomy.org/uris/models/{self.id}')
        yield self.iri, rdf.type, readable.Graph
        yield self.iri, readable.name, rdflib.Literal(self.name)
        yield self.iri, readable.abbreviation, rdflib.Literal(self.abbreviation)
        externals = []
        for id, blob in self.resources.items():
            if 'class' not in blob:
                logd.warning(f'no class in\n{blob!r} for {id}')
                continue
            elif blob['class'] == 'Graph':
                continue

            obj = getattr(self, blob['class'])(blob, self.context, self.label_suffix)

            if blob['class'] == 'External':
                # defer lookup
                externals.append(obj)
                continue

            yield from obj.triples()

        Async()(deferred(lambda x: x._term)(e) for e in externals)
        for e in externals:
            yield from e.triples()

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
    objects_ordered = tuple()
    objects_ordered_succession = MappingProxyType({})

    def triples(self):
        yield from self.triples_resource()
        yield from self.triples_external()
        yield from self.triples_annotations()
        yield from self.triples_generics()
        yield from self.triples_objects()
        yield from self.triples_objects_multi()
        yield from self.triples_objects_ordered()

    def triples_resource(self):
        yield self.s, rdf.type, owl.NamedIndividual
        yield self.s, rdf.type, elements[f'{self.cname}']
        yield self.s, readable['name'], rdflib.Literal(self.name)
        yield self.s, rdfs.label, rdflib.Literal(self.name + self.label_suffix)

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
                # TODO the logic for warnings is a bit more complex
                # e.g. when should a lyph have a topology? not entirely clear
                # is there a default topology?
                if key not in ('topology', 'conveyingType'):
                    log.warning(f'{key} not in {self.blob}')

    def triples_objects(self):
        for key in self.objects:
            if key in self.blob:
                value = self.blob[key]
                value = value.replace(' ', '-')  # FIXME require no spaces in internal ids
                o = self.context[value]

                #if key == 'source':
                    #yield o, readable.sourceOf, self.s
                #elif key == 'target':
                    #yield o, readable.targetOf, self.s

                yield self.s, readable[key], o

    def triples_objects_multi(self):
        for key in self.objects_multi:
            if key in self.blob:
                values = self.blob[key]
                assert not isinstance(values, str), f'{values} in {key}'
                for value in values:
                    if key == 'external':
                        o = OntId(value).URIRef
                        yield o, readable.annotates, self.s
                    elif key == 'inheritedExternal':
                        o = OntId(value).URIRef
                    else:
                        value = value.replace(' ', '-')  # FIXME require no spaces in internal ids
                        o = self.context[value]

                    yield self.s, readable[key], o

    def triples_objects_ordered(self):
        for key, predicate in self.objects_ordered_succession.items():
            if key in self.blob:
                values = self.blob[key]
                if values:
                    assert not isinstance(values, str), f'{values} in {key}'
                    objects = [OntId(self.context[v.replace(' ', '-')]).URIRef for v in values]
                    for s, o in zip(objects[:-1],objects[1:]):
                        yield s, predicate, o

        for key in self.objects_ordered:
            if key in self.blob:
                values = self.blob[key]
                if values:
                    assert not isinstance(values, str), f'{values} in {key}'
                    objects = [OntId(self.context[v.replace(' ', '-')]).URIRef for v in values]
                    yield from cmb.olist(*objects)(self.s, ordered[key])  # NOTE scigraph does not translate rdf lists

    def triples_external(self):
        if 'externals' in self.blob:
            for external in self.blob['external']:
                yield self.s, rdf.type, OntId(external).URIRef


class Node(BaseElement):
    key = 'nodes'
    annotations = 'skipLabel', 'color', 'generated'
    objects = 'cloneOf', 'hostedBy', 'internalIn',
    objects_multi = 'sourceOf', 'targetOf', 'clones', 'external', 'rootOf', 'leafOf',

    def triples(self):
        yield from super().triples()


Graph.Node = Node
class Lyph(BaseElement):
    key = 'lyphs'
    generics = 'topology',
    annotations = 'width', 'height', 'layerWidth', 'internalLyphColumns', 'isTemplate', 'generated'
    objects = 'layerIn', 'conveys', 'border', 'cloneOf', 'supertype', 'internalIn'
    objects_multi = ('inCoalescences', 'subtypes', 'layers', 'clones', 'external', 'inheritedExternal'
                     'internalNodes', 'bundles', 'bundlesTrees', 'bundlesChains', 'subtypes')

    def triples(self):
        yield from super().triples()


Graph.Lyph = Lyph
class Link(BaseElement):
    key = 'links'
    annotations = 'generated',
    generics = 'conveyingType',
    objects = 'source', 'target', 'conveyingLyph', 'fasciculatesIn'
    objects_multi = 'conveyingMaterials', 'hostedNodes', 'external', 'inheritedExternal',  'next', 'prev'


Graph.Link = Link
class Coalescence(BaseElement):
    key = 'coalescences'
    generics = 'topology',
    annotations = 'generated',
    objects = 'generatedFrom',
    objects_multi = 'lyphs', 'external', 'inheritedExternal'


Graph.Coalescence = Coalescence
class Border(BaseElement):
    # FIXME class is Link ?
    key = 'borders'
    objects = 'host',
    objects_multi = 'borders', 'external', 'inheritedExternal'


Graph.Border = Border
class Tree(BaseElement):
    key = 'trees'
    objects = 'root', 'lyphTemplate', 'group'
    objects_multi = 'housingLyphs', 'external', 'levels', 'inheritedExternal'


Graph.Tree = Tree
class Chain(BaseElement):
    key = 'chains'
    #internal_references = 'housingLayers',   # FIXME TODO
    objects = 'root', 'leaf', 'lyphTemplate', 'group', 'housingChain'
    objects_multi = 'housingLyphs', 'external', 'levels', 'lyphs', 'inheritedExternal'
    #objects_ordered_succession = {'lyphs': readable.nextLyph,
                                  #'levels': readable['next']}


Graph.Chain = Chain
class Group(BaseElement):
    key = 'groups'
    objects = 'generatedFrom',
    objects_multi = 'nodes', 'links', 'Lyphs', 'coalescences', 'groups'
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
    objects_multi = 'materials', 'inMaterials', 'external', 'inheritedExternal'


class fake:
    """ filler """
    label = 'sigh'


Graph.Material = Material
class External(BaseElement):
    externals = 'id',
    annotations = 'generated', 'uri', 'type',  # FIXME should be classes
    objects_multi = 'annotates',

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
            self._c_term = fake
            self.s = OntId(self.id).URIRef
            return

        self.s = OntId(self.id).URIRef

    @property
    def _term(self):
        if not hasattr(self, '_c_term'):
            self._c_term = OntTerm(self.id)

        return self._c_term

    def triples_resource(self):
        yield self.s, rdf.type, owl.Class
        l = self._term.label if self._term.label is not None else self._term.curie
        yield self.s, rdfs.label, rdflib.Literal(l)
        # TODO triples simple?


Graph.External = External
class Channel(BaseElement):
    pass


Graph.Channel = Channel


if __name__ == '__main__':
    # FIXME this eternal annoyance of dealing with the network at top level
    # of course if you want to generate some code based on something from the
    # network then of course you have to do this at some point otherwise
    # you have to figure out how to defer loading until the last possible moment
    apinscm = sc.ApiNATOMYSchema()
    hrm = make_classes(apinscm.schema)
