from datetime import datetime
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
                                   dc,)
from sparcur import converters as conv
from sparcur.core import (adops,
                          OntCuries,
                          OntId,
                          curies_runtime,)
from sparcur.utils import loge, log, BlackfynnId
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
    def header_label(self):
        return rdflib.Literal(f'{self.folder_name} curation export graph')

    def _triples_header(self, ontid, nowish):
        epoch = int(nowish.timestamp())  # truncate to second to match iso
        iso = isoformat(nowish)
        ver_ontid = rdflib.URIRef(ontid + f'/version/{epoch}/{self.id}')
        sparc_methods = rdflib.URIRef('https://raw.githubusercontent.com/SciCrunch/'
                                      'NIF-Ontology/sparc/ttl/sparc-methods.ttl')
        sparc_mis_helper = rdflib.URIRef('https://raw.githubusercontent.com/SciCrunch/'
                                         'NIF-Ontology/sparc/ttl/sparc-mis-helper.ttl')
        pos = (
            (rdf.type, owl.Ontology),
            (isAbout, rdflib.URIRef(self.uri_api)),
            (TEMP.hasUriHuman, rdflib.URIRef(self.uri_human)),
            (owl.imports, sparc_methods),
            (owl.imports, sparc_mis_helper),

            (owl.versionIRI, ver_ontid),
            (owl.versionInfo, rdflib.Literal(iso)),

            (TEMP.TimestampExportStart, rdflib.Literal(self.timestamp_export_start)),
            (rdfs.label, self.header_label),
            (rdfs.comment, self.header_graph_description),
        )
        for p, o in pos:
            yield ontid, p, o

    @property
    def triples_header(self):
        ontid = self.ontid
        nowish = utcnowtz()
        yield from self._triples_header(ontid, nowish)

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
        if hasattr(self, 'uri_api'):
            base = self.uri_api + '/'
            graph.namespace_manager.populate_from(curies_runtime(base))

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
    def performances(self):
        if 'performances' in self.data:
            yield from self.data['performances']

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
            for i, c in enumerate(data['contributors']):
                creator = ('name' in c and
                           [cr for cr in creators if 'name' in cr and cr['name'] == c['name']])
                yield from self.triples_contributors(c, i, creator=creator)

    def triples_contributors(self, contributor, contributor_order_index, creator=False):
        try:
            dsid = self.dsid  # FIXME json reload needs to deal with this
        except BaseException as e:  # FIXME ...
            loge.exception(e)
            return

        cid = contributor['id']

        if isinstance(cid, idlib.Stream) and hasattr(cid, 'asUri'):  # FIXME nasty branch
            s = cid.asUri(rdflib.URIRef)
        elif isinstance(cid, BlackfynnId):
            s = rdflib.URIRef(cid.uri_api)
        elif isinstance(cid, dict):
            if isinstance(cid['id'], idlib.Stream):  # FIXME nasty branch
                s = cid['id'].asUri(rdflib.URIRef)
            else:
                raise NotImplementedError(f'{type(cid["id"])}: {cid["id"]}')
        else:
            s = rdflib.URIRef(cid)  # FIXME json reload needs to deal with this

        if 'data_remote_user_id' in contributor:
            userid = rdflib.URIRef(contributor['data_remote_user_id'].uri_api)  # FIXME
            yield s, TEMP.hasDataRemoteUserId, userid

        if 'blackfynn_user_id' in contributor:
            userid = rdflib.URIRef(contributor['blackfynn_user_id'].uri_api)  # FIXME
            yield s, TEMP.hasBlackfynnUserId, userid

        yield s, rdf.type, owl.NamedIndividual
        yield s, rdf.type, sparc.Person
        yield s, TEMP.contributorTo, dsid  # TODO other way around too? hasContributor
        converter = conv.ContributorConverter(contributor)
        yield from converter.triples_gen(s)
        if creator:
            yield s, TEMP.creatorOf, dsid

        # dataset <-> contributor object
        dcs = rdflib.BNode()

        yield dcs, rdf.type, owl.NamedIndividual
        yield dcs, rdf.type, sparc.DatasetContribution
        yield dcs, TEMP.aboutDataset, dsid  # FIXME forDataset?
        yield dcs, TEMP.aboutContributor, s
        yield dcs, TEMP.contributorOrderIndex, rdflib.Literal(contributor_order_index)
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

        for key, convclass in (('meta', conv.MetaConverter),
                               ('rmeta', conv.RmetaConverter),
                               ('submission', conv.SubmissionConverter)
                               ):
            if key in data:
                converter = convclass(data[key], self)
                yield from converter.triples_gen(dsid)
            else:
                loge.warning(f'{self} has no {key}!')  # FIXME split logs into their problems, and our problems
        if 'status' not in data:
            breakpoint()

        yield from conv.StatusConverter(data['status'], self).triples_gen(dsid)

        #converter = conv.DatasetConverter(data)
        #yield from converter.triples_gen(dsid)

        def id_(v):
            s = rdflib.URIRef(dsid)
            yield s, rdf.type, owl.NamedIndividual
            yield s, rdf.type, sparc.Dataset
            yield s, rdfs.label, rdflib.Literal(self.folder_name)  # not all datasets have titles

        yield from id_(self.id)

        #for subjects in self.subjects:
            #for s, p, o in subjects.triples_gen(subject_id):
                #if type(s) == str:
                    #breakpoint()
                #yield s, p, o

        yield from self.ddt(data)
        yield from self.triples_performances
        yield from self.triples_subjects
        yield from self.triples_samples
        yield from self.triples_specimen_dirs

    def _psd(self, rec, dsi):
        type = rec['type']
        spec_id = rec['specimen_id']
        if type == 'SampleDirs':
            sid = self.primary_key(spec_id)
        elif type == 'SubjectDirs':
            sid = self.subject_id(spec_id)
        else:
            raise NotImplementedError(f'wat {type}')

        for drp in rec['dirs']:
            path_record = dsi[drp]
            collection_id = path_record['remote_id']
            #p = (self.data['prov']['export_project_path'] /
                 #self.data['meta']['folder_name'] /
                 #drp)
            #cid = p.cache.cache.uri_api
            cid = OntId(collection_id).u
            yield sid, TEMP.hasFolderAboutIt, cid
            # TEMP.isFolderAboutSpecimen
            # NOTE the details for these do not go here
            #yield cid, rdf.type, owl.NamedIndividual
            #yield cid, rdf.type, sparc.Folder
            #yield cid, TEMP.isAboutSpecimen, sid

    @property
    def triples_specimen_dirs(self):
        ds = self.data['dir_structure']
        dsi = {b['dataset_relative_path']:b for b in ds}

        if 'specimen_dirs' in self.data:
            # not everyone has these, and sometimes if the metadata
            # files are mangled enough they don't even have subjects
            # or samples that would cause errors
            sd = self.data['specimen_dirs']
            if 'records' in sd:
                recs = sd['records']
                for rec in recs:
                    yield from self._psd(rec, dsi)

    def _thing_id(self, v, path):
        if not isinstance(v, str):
            #loge.critical('darn it max normlize your ids!')  # now caught by the schemas
            if isinstance(v, datetime):
                # XXX datetime is a REALLY bad case because there is not a
                # canonical representation
                v = isoformat(v)
            else:
                v = str(v)

        v = quote(v, safe=tuple())

        s = rdflib.URIRef(self.dsid + path + v)
        return s

    def performance_id(self, v):
        return self._thing_id(v, '/performances/')

    @property
    def triples_performances(self):
        try:
            dsid = self.dsid  # FIXME json reload needs to deal with this
        except BaseException as e:  # FIXME ...
            loge.exception(e)
            return

        def triples_gen(prefix_func, performances):

            for i, performance in enumerate(performances):
                converter = conv.PerformanceConverter(performance)
                if 'performance_id' in performance:
                    s_local = performance['performance_id']
                else:
                    s_local = f'local-{i + 1}'  # sigh

                s = prefix_func(s_local)
                yield s, rdf.type, owl.NamedIndividual
                yield s, rdf.type, sparc.Performance
                yield s, TEMP.hasDerivedInformation, dsid
                yield dsid, TEMP.isAboutPerformance, s
                yield from converter.triples_gen(s, raw_keys=True)

        yield from triples_gen(self.performance_id, self.performances)

    def subject_id(self, v, species=None):  # TODO species for human/animal
        return self._thing_id(v, '/subjects/')

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
                yield from converter.triples_gen(s, raw_keys=True)

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
                yield from converter.triples_gen(s, raw_keys=True)
                # see https://github.com/information-artifact-ontology/IAO/issues/60, there isn't a good inverse relation
                # original though was subjectOfInformation, but that was confusing in the current terminology where subject already has 2 meanings
                # hasInformationDerivedFromProcessWhereWasParticipant -> hasInformationDerivedFromProcessWhereWasPrimaryParticipant seems most correct, but is extremely verbose
                # hasDerivedInformationAsParticipant -> hasDerivedInformationAsParticipantPrimary materialize the role into the predicate? seems reasonable
                continue

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
        epoch = int(nowish.timestamp())  # truncate to second to match iso
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
