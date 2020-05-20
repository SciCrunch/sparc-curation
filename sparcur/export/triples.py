from urllib.parse import quote
import idlib
import rdflib
from pyontutils.core import OntGraph
from pyontutils.utils import isoformat, utcnowtz
from pyontutils.namespaces import (TEMP,
                                   isAbout,
                                   sparc,
                                   rdf,
                                   rdfs,
                                   owl,
                                   skos,
                                   dc,)
from sparcur import converters as conv
from sparcur.core import (adops,
                          OntCuries)
from sparcur.utils import loge, log
from sparcur.protocols import ProtcurData


class TriplesExport(ProtcurData):

    def __init__(self, data_json, *args, teds=tuple(), **kwargs):
        self.data = data_json
        self.teds = teds
        self.id = self.data['id']
        self.uri_api = self.data['meta']['uri_api']
        self.uri_human = self.data['meta']['uri_human']
        self.folder_name = self.data['meta']['folder_name']
        #self.title = self.data['meta']['title'] if 

    @property
    def timestamp_export_start(self):
        return adops.get(self.data, ['prov', 'timestamp_export_start'])

    @property
    def protocol_uris(self):  # FIXME this needs to be pipelined
        try:
            yield from adops.get(self.data, ['meta', 'protocol_url_or_doi'])
        except exc.NoSourcePathError:
            pass

    @property
    def ontid(self):
        return rdflib.URIRef(f'https://cassava.ucsd.edu/sparc/ontologies/{self.id}')

    @property
    def header_graph_description(self):
        raise NotImplementedError

    @property
    def triples_header(self):
        ontid = self.ontid
        nowish = utcnowtz()
        epoch = nowish.timestamp()
        iso = isoformat(nowish)
        ver_ontid = rdflib.URIRef(ontid + f'/version/{epoch}/{self.id}')
        sparc_methods = rdflib.URIRef('https://raw.githubusercontent.com/SciCrunch/'
                                      'NIF-Ontology/sparc/ttl/sparc-methods.ttl')

        pos = (
            (rdf.type, owl.Ontology),
            (owl.versionIRI, ver_ontid),
            (owl.versionInfo, rdflib.Literal(iso)),
            (isAbout, rdflib.URIRef(self.uri_api)),
            (TEMP.hasHumanUri, rdflib.URIRef(self.uri_human)),
            (rdfs.label, rdflib.Literal(f'{self.folder_name} curation export graph')),
            (rdfs.comment, self.header_graph_description),
            (owl.imports, sparc_methods),
            (TEMP.TimestampExportStart, rdflib.Literal(self.timestamp_export_start)),
        )
        for p, o in pos:
            yield ontid, p, o

    @property
    def graph(self):
        """ you can populate other graphs, but this one runs once """
        if not hasattr(self, '_graph'):
            graph = OntGraph()
            self.populate(graph)
            self._graph = graph

        return self._graph

    @property
    def ttl(self):
        return self.graph.serialize(format='nifttl')

    def populate(self, graph):
        def warn(triple):
            for element in triple:
                if (not (isinstance(element, rdflib.URIRef) or
                         isinstance(element, rdflib.BNode) or
                         isinstance(element, rdflib.Literal)) or
                    (hasattr(element, '_value') and (isinstance(element._value, dict) or
                                                     isinstance(element._value, list) or
                                                     isinstance(element._value, tuple))) or
                    (isinstance(element, rdflib.URIRef) and (element.startswith('<') or
                                                             not rdflib.term._is_valid_uri(element)))):
                    #if (isinstance(element, rdflib.URIRef) and element.startswith('<')):
                        #breakpoint()
                    loge.critical(element)

            return triple

        OntCuries.populate(graph)  # ah smalltalk thinking
        [graph.add(t) for t in self.triples_header if warn(t)]
        for t in self.triples:
            if warn(t):
                graph.add(t)


class TriplesExportSummary(TriplesExport):
    def __iter__(self):
        yield from self.data['datasets']

    @property
    def header_graph_description(self):
        return rdflib.Literal(f'SPARC organization graph for {self.id}')

    @property
    def triples(self):
        if self.teds:
            # TODO conjuctive graph?
            for ted in self.teds:
                # FIXME BNode collision risk? Probably not?
                # FIXME yeah, figuring out conjuctive graph for header plus
                sont = next(ted.graph[:rdf.type:owl.Ontology])
                for s, p, o in ted.graph:
                    if s == sont:
                        continue

                    yield s, p, o

        else:
            for dataset_blob in self:
                ted = TriplesExportDataset(dataset_blob)
                yield from ted.triples


class TriplesExportDataset(TriplesExport):
    @property
    def header_graph_description(self):
        return rdflib.Literal(f'SPARC single dataset graph for {self.id}')

    @property
    def subjects(self):
        if 'subjects' in self.data:
            yield from self.data['subjects']

    @property
    def samples(self):
        if 'samples' in self.data:
            yield from self.data['samples']

    @property
    def dsid(self):
        return rdflib.URIRef(self.uri_api)

    def ddt(self, data):
        if 'contributors' in data:
            if 'creators' in data:
                creators = data['creators']
            else:
                creators = []
            for c in data['contributors']:
                creator = ('name' in c and
                           [cr for cr in creators if 'name' in cr and cr['name'] == c['name']])
                yield from self.triples_contributors(c, creator=creator)

    def triples_contributors(self, contributor, creator=False):
        try:
            dsid = self.dsid  # FIXME json reload needs to deal with this
        except BaseException as e:  # FIXME ...
            loge.exception(e)
            return

        cid = contributor['id']

        if isinstance(cid, idlib.Stream):  # FIXME nasty branch
            s = cid.asUri(rdflib.URIRef)
        elif isinstance(cid, dict):
            if isinstance(cid['id'], idlib.Stream):  # FIXME nasty branch
                s = cid['id'].asUri(rdflib.URIRef)
            else:
                raise NotImplementedError(f'{type(cid["id"])}: {cid["id"]}')
        else:
            s = rdflib.URIRef(cid)  # FIXME json reload needs to deal with this

        if 'blackfynn_user_id' in contributor:
            userid = rdflib.URIRef(contributor['blackfynn_user_id'])
            yield s, TEMP.hasBlackfynnUserId, userid

        yield s, rdf.type, owl.NamedIndividual
        yield s, rdf.type, sparc.Researcher
        yield s, TEMP.contributorTo, dsid  # TODO other way around too? hasContributor
        converter = conv.ContributorConverter(contributor)
        yield from converter.triples_gen(s)
        if creator:
            yield s, TEMP.creatorOf, dsid

        # dataset <-> contributor object
        dcs = rdflib.BNode()

        yield dcs, rdf.type, owl.NamedIndividual
        yield dcs, rdf.type, TEMP.DatasetContributor
        yield dcs, TEMP.aboutDataset, dsid  # FIXME forDataset?
        yield dcs, TEMP.aboutContributor, s
        dconverter = conv.DatasetContributorConverter(contributor)
        for _s, p, o in dconverter.triples_gen(dcs):
            if p == sparc.isContactPerson and o._value == True:
                yield dsid, TEMP.hasContactPerson, s
            yield _s, p, o

    @property
    def triples(self):
        # FIXME ick
        data = self.data
        try:
            dsid = self.uri_api
        except BaseException as e:  # FIXME ...
            raise e
            return

        if 'meta' in data:
            meta_converter = conv.MetaConverter(data['meta'], self)
            yield from meta_converter.triples_gen(dsid)
        else:
            loge.warning(f'{self} has no meta!')  # FIXME split logs into their problems, and our problems

        if 'submission' in data:
            submission_converter = conv.SubmissionConverter(data['submission'], self)
            yield from submission_converter.triples_gen(dsid)
        else:
            loge.warning(f'{self} has no submission!')  # FIXME split logs into their problems, and our problems

        if 'status' not in data:
            breakpoint()

        yield from conv.StatusConverter(data['status'], self).triples_gen(dsid)

        #converter = conv.DatasetConverter(data)
        #yield from converter.triples_gen(dsid)

        def id_(v):
            s = rdflib.URIRef(dsid)
            yield s, rdf.type, owl.NamedIndividual
            yield s, rdf.type, sparc.Resource
            yield s, rdfs.label, rdflib.Literal(self.folder_name)  # not all datasets have titles

        yield from id_(self.id)

        #for subjects in self.subjects:
            #for s, p, o in subjects.triples_gen(subject_id):
                #if type(s) == str:
                    #breakpoint()
                #yield s, p, o

        yield from self.ddt(data)
        yield from self.triples_subjects
        yield from self.triples_samples

    def subject_id(self, v, species=None):  # TODO species for human/animal
        if isinstance(v, int):
            loge.critical('darn it max normlize your ids!')
            v = str(v)

        v = quote(v, safe=tuple())
        s = rdflib.URIRef(self.dsid + '/subjects/' + v)
        return s

    @property
    def triples_subjects(self):
        try:
            dsid = self.dsid  # FIXME json reload needs to deal with this
        except BaseException as e:  # FIXME ...
            loge.exception(e)
            return

        def triples_gen(prefix_func, subjects):

            for i, subject in enumerate(subjects):
                converter = conv.SubjectConverter(subject)
                if 'subject_id' in subject:
                    s_local = subject['subject_id']
                else:
                    s_local = f'local-{i + 1}'  # sigh

                s = prefix_func(s_local)
                yield s, rdf.type, owl.NamedIndividual
                yield s, rdf.type, sparc.Subject
                yield s, TEMP.hasDerivedInformationAsParticipant, dsid
                yield dsid, TEMP.isAboutParticipant, s
                yield from converter.triples_gen(s)
                continue
                for field, value in subject.items():
                    convert = getattr(converter, field, None)
                    if convert is not None:
                        yield (s, *convert(value))
                    elif field not in converter.known_skipped:
                        loge.warning(f'Unhandled subject field: {field}')

        yield from triples_gen(self.subject_id, self.subjects)

    def primary_key(self, v, species=None):  # TODO species for human/animal
        #v = v.replace(' ', '%20')  # FIXME use quote urlencode
        v = quote(v, safe=tuple())
        s = rdflib.URIRef(self.dsid + '/samples/' + v)
        return s

    @property
    def triples_samples(self):
        try:
            dsid = self.dsid  # FIXME json reload needs to deal with this
        except BaseException as e:  # FIXME ...
            loge.exception(e)
            return

        conv.SampleConverter._subject_id = self.subject_id  # FIXME
        conv.SampleConverter.dsid = self.dsid  # FIXME FIXME very evil
        # yes this indicates that converters and exporters are
        # highly related here ...
        def triples_gen(prefix_func, samples):
            for i, sample in enumerate(samples):
                converter = conv.SampleConverter(sample)
                if 'primary_key' in sample:
                    s_local = sample['primary_key']
                else:
                    s_local = f'local-{i + 1}'  # sigh

                s = prefix_func(s_local)
                yield s, rdf.type, owl.NamedIndividual
                yield s, rdf.type, sparc.Sample
                yield s, TEMP.hasDerivedInformationAsParticipant, dsid  # domain particiant range information artifact
                # specimen - participant: -> process instance - ilxtr:hasInformationOutput -> data files - partOf: -> dataset
                # collapses to? specimen - hasInformationDerivedFromProce -> <- containsInformationAbout - dataset
                yield dsid, TEMP.isAboutParticipant, s  # containsInformationAboutParticipant[Primary] TEMP.containsInformationAbout, isAbout is probably a better base
                # could be further refined to isAboutParticiantPrimary, with a note that if multiple measurement processes happened, there can be multiple primaries for a dataset
                yield from converter.triples_gen(s)
                # see https://github.com/information-artifact-ontology/IAO/issues/60, there isn't a good inverse relation
                # original though was subjectOfInformation, but that was confusing in the current terminology where subject already has 2 meanings
                # hasInformationDerivedFromProcessWhereWasParticipant -> hasInformationDerivedFromProcessWhereWasPrimaryParticipant seems most correct, but is extremely verbose
                # hasDerivedInformationAsParticipant -> hasDerivedInformationAsParticipantPrimary materialize the role into the predicate? seems reasonable
                continue
                for field, value in sample.items():
                    convert = getattr(converter, field, None)
                    if convert is not None:
                        yield (s, *convert(value))
                    elif field not in converter.known_skipped:
                        loge.warning(f'Unhandled sample field: {field}')

        yield from triples_gen(self.primary_key, self.samples)


class TriplesExportIdentifierMetadata(TriplesExport):
    def __init__(self, data_json, *args, **kwargs):
        self.data = data_json
        self.id = self.data['id']

    @property
    def header_graph_description(self):
        return rdflib.Literal(f'Additional metadata for identifiers referenced in export.')

    @property
    def triples_header(self):
        ontid = self.ontid
        nowish = utcnowtz()
        epoch = nowish.timestamp()
        iso = isoformat(nowish)
        ver_ontid = rdflib.URIRef(ontid + f'/version/{epoch}/{self.id}')
        pos = (
            (rdf.type, owl.Ontology),
            (owl.versionIRI, ver_ontid),
            (owl.versionInfo, rdflib.Literal(iso)),
            (rdfs.comment, self.header_graph_description),
            (TEMP.TimestampExportStart, rdflib.Literal(self.timestamp_export_start)),
        )
        for p, o in pos:
            yield ontid, p, o

    def published_online(self, blob):
        try:
            dpl = blob['issued']['date-parts']
        except KeyError as e:
            log.critical(e)
            return None

        dp = dpl[0]
        if len(dp) == 3:
            y, m, d = dp
            return f'{y}-{m:0>2}-{d:0>2}'
        elif len(dp) == 2:
            y, m = dp
            return f'{y}-{m:0>2}'
        elif len(dp) == 1:
            y = dp
            return f'{y}'
        else:
            raise NotImplementedError(f'what the? {dp}')

    @property
    def triples(self):
        crossref_doi_pred = rdflib.term.URIRef('http://prismstandard.org/namespaces/basic/2.1/doi')
        for blob in self.data['identifier_metadata']:
            id = blob['id']
            if not isinstance(id, idlib.Stream):
                id = idlib.Auto(id)

            if not hasattr(id, 'asUri'):
                breakpoint()

            s = id.asUri(rdflib.URIRef)
            if 'source' in blob:
                source = blob['source']  # FIXME we need to wrap this in our normalized representation
                if source == 'Crossref':  # FIXME CrossrefConvertor etc. OR put it in idlib as a an alternate ttl
                    pos = (
                        (rdf.type, owl.NamedIndividual),
                        (rdf.type, TEMP[blob['type']]),
                        (dc.publisher, blob['publisher']),
                        #(dc.type, blob['type']),  # FIXME semantify
                        (dc.title, blob['title']),
                        (dc.date, self.published_online(blob)),  # FIXME .... dangerzone
                    )
                    g = OntGraph()
                    doi = idlib.Doi(id) if not isinstance(id, idlib.Doi) else id  # FIXME idlib streams need to recognize their own type in __new__
                    data = doi.ttl()
                    if data is None:  # blackfynn has some bad settings on their doi records ...
                        return

                    try:
                        g.parse(data=data, format='ttl')  # FIXME network bad
                    except BaseException as e:
                        loge.exception(e)

                    _tr = [s for s, p, o in g if p == crossref_doi_pred]
                    if _tr:
                        _their_record_s = _tr[0]
                        yield s, owl.sameAs, _their_record_s
                        yield from g
                    else:
                        g.debug()
                        log.critical('No crossref doi section in graph!')
                else:
                    msg = f'dont know what to do with {source}'
                    log.error(msg)
                    #raise NotImplementedError(msg)
                    return
            else:
                msg = f'dont know what to do with {blob} for {id.identifier}'
                log.error(msg)
                #raise NotImplementedError(msg)
                return

            for p, oraw in pos:
                if oraw is not None:
                    o = rdflib.Literal(oraw) if not isinstance(oraw, rdflib.URIRef) else oraw
                    yield s, p, o
