from types import GeneratorType
import idlib
import rdflib
from pysercomb.pyr.types import ProtcurExpression, Quantity, Range
from pyontutils import combinators as cmb
from pyontutils.namespaces import TEMP, TEMPRAW, isAbout, sparc, NIFRID
from pyontutils.namespaces import rdf, rdfs, owl, dc
from sparcur import datasets as dat
from sparcur import exceptions as exc
from sparcur.core import OntId, OntTerm, lj, fromJson
from sparcur.utils import log, logd
from sparcur.protocols import ProtocolData


class TripleConverter(dat.HasErrors):
    # TODO consider putting mappings in a dict hierarchy
    # that reflects where they are in the schema??
    known_skipped = tuple()
    mapping = tuple()

    _already_logged = set()

    class Extra:
        def __init__(self, converter):
            self.c = converter
            self.integrator = converter.integrator

    @classmethod
    def setup(cls):
        for attr, predicate in cls.mapping:
            def _func(self, value, p=predicate): return p, self.l(value)
            setattr(cls, attr, _func)

    def __init__(self, json_source, integrator=None):
        """ in case we want to do contextual things here """
        super().__init__()
        self._source = json_source
        self.integrator = integrator
        self.extra = self.Extra(self)

    def l(self, value):
        if isinstance(value, idlib.Stream) and hasattr(value, '_id_class'):
            if hasattr(value, 'asUri'):  # FIXME
                return value.asUri(rdflib.URIRef)
            else:
                return value.asType(rdflib.URIRef)
        if isinstance(value, OntId):
            return value.u
        if isinstance(value, ProtcurExpression):
            return value
        if isinstance(value, Quantity):
            return value
        elif isinstance(value, str) and value.startswith('http'):
            return OntId(value).u
        elif isinstance(value, dict):  # FIXME this is too late to convert?
            # NOPE! This idiot put a type field in his json dicts!
            if 'type' in value:
                if value['type'] == 'quantity':
                    return self.pyru._Quant.fromJson(value)
                elif value['type'] == 'range':
                    return self.pyru.Range.fromJson(value)
                elif value['type'] == 'identifier':
                    return fromJson(value).asType(rdflib.URIRef)

            raise ValueError(value)
        else:
            return rdflib.Literal(value)

    def triples_gen(self, subject):
        if not (isinstance(subject, rdflib.URIRef) or
                isinstance(subject, rdflib.BNode)):
            if isinstance(subject, idlib.Stream):
                subject = subject.asType(rdflib.URIRef)
            else:
                subject = rdflib.URIRef(subject)

        #maybe_not_normalized = self.message_passing_key in self._source  # TODO maybe not here?
        for field, value in self._source.items():
            #normalized = not (maybe_not_normalized and field in self._source)  # TODO

            #log.debug(f'{field}: {value}')
            if type(field) is object:
                continue  # the magic helper key for Pipeline
            convert = getattr(self, field, None)
            extra = getattr(self.extra, field, None)
            if convert is not None:
                if isinstance(value, tuple) or isinstance(value, list):
                    values = value
                else:
                    values = value,

                for v in values:
                    #log.debug(f'{field} {v} {convert}')
                    try:
                        p, o = convert(v)
                    except exc.NoTripleError as e:
                        continue

                    #log.debug((o, v))
                    a = (isinstance(o, idlib.Stream) and hasattr(o, '_id_class')
                         or isinstance(o, OntTerm))
                    b = (isinstance(v, idlib.Stream) and hasattr(v, '_id_class')
                         or isinstance(v, OntTerm))
                    if (a or b):
                        # FIXME this thing is a mess ...
                        _v = o if a else v
                        s = _v.asUri(rdflib.URIRef)
                        yield subject, p, s
                        try:
                            d = _v.asDict()  # FIXME this is a silly way to do this, use Stream.triples_gen
                            _o = (owl.Class
                                  if isinstance(v, OntTerm) else  # FIXME not really accurate
                                  owl.NamedIndividual)
                            yield s, rdf.type, _o
                            if 'label' in d:
                                yield s, rdfs.label, rdflib.Literal(d['label'])
                            if 'synonyms' in d:  # FIXME yield from o.synonyms(s)
                                for syn in d['synonyms']:
                                    yield s, NIFRID.synonym, rdflib.Literal(syn)
                        except idlib.exceptions.ResolutionError:
                            pass

                    elif isinstance(o, ProtcurExpression) or isinstance(o, Quantity):
                        s = rdflib.BNode()
                        yield subject, p, s
                        qt = sparc.Measurement
                        if isinstance(o, Range):
                            yield from o.asRdf(s, quantity_rdftype=qt)
                        elif isinstance(o, Quantity):
                            yield from o.asRdf(s, rdftype=qt)
                            n = rdflib.BNode()
                            yield s, TEMP.asBaseUnits, n
                            yield from o.to_base_units().asRdf(n)
                        else:
                            log.warning(f'unhanded Expr type {o}')
                            yield from o.asRdf(s)
                    else:
                        yield subject, p, o

                    if extra is not None:
                        yield from extra(v)

            elif field in self.known_skipped:
                pass

            else:
                msg = f'Unhandled {self.__class__.__name__} field: {field}'
                if self.addError(msg, pipeline_stage=self.__class__.__name__ + '.export-error'):
                    log.warning(msg)


class ContributorConverter(TripleConverter):
    known_skipped = 'id', 'contributor_name'
    mapping = (
        ('first_name', sparc.firstName),
        ('middle_name', TEMP.middleName),
        ('last_name', sparc.lastName),
        ('contributor_affiliation', TEMP.hasAffiliation),
        ('affiliation', TEMP.hasAffiliation),  # FIXME iri vs literal
        ('blackfynn_user_id', TEMP.hasBlackfynnUserId),
        ('contributor_orcid_id', sparc.hasORCIDId),
        )
 
    class Extra:
        def __init__(self, converter):
            self.c = converter
            self.integrator = converter.integrator

        def affiliation(self, value):
            #_, s = self.c.affiliation(value)
            if isinstance(value, str):  # FIXME json conv
                yield from idlib.Ror(value).triples_gen
            else:
                yield from value.triples_gen
               

ContributorConverter.setup()

class DatasetContributorConverter(TripleConverter):
    known_skipped = 'id', 'contributor_name', 'first_name', 'last_name'

    mapping = (
        ('is_contact_person', sparc.isContactPerson),
        ('is_responsible_pi', sparc.isContactPerson),
    )

    def contributor_role(self, value):
        return TEMP.hasRole, TEMP[value]

DatasetContributorConverter.setup()

class MetaConverter(TripleConverter):
    mapping = [
        ['acknowledgements', TEMP.acknowledgements],
        ['folder_name', rdfs.label],
        ['title', dc.title],
        ['protocol_url_or_doi', TEMP.hasProtocol],
        #['award_number', TEMP.hasAwardNumber],
        ['species', isAbout],
        ['organ', isAbout],
        ['modality', TEMP.hasExperimentalModality],
        ['techniques', TEMP.protocolEmploysTechnique],
        ['uri_api', TEMP.hasUriApi],
        ['uri_human', TEMP.hasUriHuman],
        ['keywords', isAbout],
        ['description', dc.description],
        ['dirs', TEMP.hasNumberOfDirectories],
        ['files', TEMP.hasNumberOfFiles],
        ['size', TEMP.hasSizeInBytes],
        ['subject_count', TEMP.hasNumberOfSubjects],
        ['sample_count', TEMP.hasNumberOfSamples],
        ['contributor_count', TEMP.hasNumberOfContributors],

        ['title_for_complete_data_set', TEMP.collectionTitle],
        ['prior_batch_number', TEMP.Continues],  # see datacite relationType

        ['originating_article_doi', TEMP.isDescribedBy],  # see relationType  IsDescribedBy -> isDescribedBy
        ['timestamp_created', TEMP.wasCreatedAtTime],
        ['timestamp_updated', TEMP.wasUpdatedAtTime],

        # TODO
        #['additional_links', ],
        #['completeness_of_data_set', ],
        #['examples'],
        #['links'],

    ]

    def doi(self, value):
        if value is not None:
            return TEMP.hasDoi, self.l(value)
        else:
            raise exc.NoTripleError('value was None')  # SIGH

    def principal_investigator(self, value):
        index = int(value.rsplit('/', 1)[-1])
        id = self.integrator.data['contributors'][index]['id']
        o = self.l(id)
        return TEMP.hasResponsiblePrincipalInvestigator, o  # FIXME reload -> ir

    def award_number(self, value): return TEMP.hasAwardNumber, TEMP[f'awards/{value}']
    class Extra:
        def __init__(self, converter):
            self.c = converter
            self.integrator = converter.integrator

        def award_number(self, value):
            _, s = self.c.award_number(value)
            yield s, rdf.type, owl.NamedIndividual
            yield s, rdf.type, TEMP.FundedResearchProject
            return
            o = self.integrator.organ(value)
            if o:
                if o != 'othertargets':
                    o = OntId(o)
                    if o.prefix == 'FMA':
                        ot = OntTerm(o)
                        o = next(OntTerm.query(label=ot.label, prefix='UBERON'))

                    yield s, isAbout, o.u

        def doi(self, value):
            yield from value.triples_gen

        def protocol_url_or_doi(self, value):
            #_, s = self.c.protocol_url_or_doi(value)
            #yield s, rdf.type, owl.NamedIndividual
            #yield s, rdf.type, sparc.Protocol
            log.debug(value)
            if not isinstance(value, idlib.Pio):
                if isinstance(value, idlib.Doi):
                    for t in value.triples_gen:
                        yield t

                    ds, _, _ = t
                    try:
                        pioid = value.dereference(asType=idlib.Pio)
                        s = self.c.l(pioid)
                        yield ds, TEMP.dereferencesTo, s
                        yield s, TEMP.hasDoi, ds
                    except idlib.exceptions.MalformedIdentifierError as e:
                        log.warning(e)
                        return
                else:
                    pioid = idlib.Pio(value)  # FIXME :/ should be handled in Pio directly probably?
            else:
                pioid = value

            s = self.c.l(pioid)

            # FIXME needs to be a pipeline so that we can export errors
            try:
                data = pioid.data()
            except OntId.BadCurieError as e:
                logd.error(e)  # FIXME export errors ...
                data = None

            if data:
                yield s, rdfs.label, rdflib.Literal(pioid.label)
                nsteps = len(data['steps'])
                yield s, TEMP.protocolHasNumberOfSteps, rdflib.Literal(nsteps)

            try:
                yield from self.integrator.triples_protcur(s)
            except OntId.BadCurieError as e:
                logd.error(e)  # FIXME export errors ...
MetaConverter.setup()  # box in so we don't forget


class DatasetConverter(TripleConverter):
    known_skipped = 'id', 'errors', 'inputs', 'subjects', 'meta', 'creators'
    mapping = []
DatasetConverter.setup()


class SubmissionConverter(TripleConverter):
    known_skipped = 'milestone_achieved', 'sparc_award_number'
    mapping = [
        ['milestone_completion_date', TEMP.milestoneCompletionDate],
    ]
SubmissionConverter.setup()

class StatusConverter(TripleConverter):
    known_skipped = 'submission_errors', 'curation_errors'
    mapping = [
        ['submission_index', TEMP.submissionIndex],
        ['curation_index', TEMP.curationIndex],
        ['error_index', TEMP.errorIndex],
    ]
StatusConverter.setup()

file_related = [
    # file related
    # this is about the file about the sample, not the spamle itself
    # FIXME we don't want to export this, we want to use it to point
    # to a blackfynn package id, but that processing has to come during
    # the pipeline stage not here ...
    ['sha1', TEMP.hasDigitalArtifactThatIsAboutItWithHash],
    ['filename', TEMP.hasDigitalArtifactThatIsAboutIt],  #
    ['upload_filename', TEMP.hasDigitalArtifactThatIsAboutIt],
    ['dataset_id', TEMP.providerDatasetIdentifier],  # FIXME need a global convention for this
    ]
performance_related = [

    # TODO execution -> performance
    ['experiment_number', TEMP.localExecutionNumber],  # FIXME TODO
    ['session', TEMP.localExecutionNumber],
]

utility = [
    ['comment', TEMP.providerNote],
    ['note', TEMP.providerNote],
]
class SubjectConverter(TripleConverter):
    mapping = [
        ['subject_id', TEMP.localId],
        ['ear_tag_number', TEMP.localIdAlt],
        ['age_category', TEMP.hasAgeCategory],
        ['experimental_group', TEMP.hasAssignedGroup],
        ['group', TEMP.hasAssignedGroup],
        ['treatment', TEMP.TODO],
        #['rrid_for_strain', rdf.type],  # if only
        ['rrid_for_strain', sparc.specimenHasIdentifier],  # really subClassOf strain
        ['genus', sparc.animalSubjectIsOfGenus],
        ['species', sparc.animalSubjectIsOfSpecies],
        ['strain', sparc.animalSubjectIsOfStrain],
        ['genotype', TEMP.hasGenotype],
        ['weight', sparc.animalSubjectHasWeight],
        ['initial_weight', sparc.animalSubjectHasWeight],  # TODO time
        ['mass', sparc.animalSubjectHasWeight],
        ['body_mass', sparc.animalSubjectHasWeight],  # TODO
        ['height_inches', TEMP.subjectHasHeight],  # FIXME converted in datasets sigh
        ['sex', TEMP.hasBiologicalSex],
        ['gender', sparc.hasGender],
        ['age', TEMP.hasAge],
        ['age_years', TEMP.hasAge],  # FIXME this is ok because we catch it when converting but
        # it is yet another case where things are decoupled

        ['body_mass_weight', TEMP.subjectHasWeight],  # FIXME human vs animal, sigh

        ['anesthesia', TEMP.wasAdministeredAnesthesia],
        # TODO experiment hasParticpant (some (subject wasAdministeredAnesthesia))
        # or something like that -> experimentHasSubjectUnder ...


        ['stimulation_site', sparc.spatialLocationOfModulator],  # TODO ontology
        ['stimulator', sparc.stimulatorUtilized],
        ['stimulating_electrode_type', sparc.stimulatorUtilized],  # FIXME instance vs type

        ['organism_rrid', TEMP.TODO],

        ['nerve', TEMPRAW.involvesAnatomicalRegion],  # FIXME we can be more specific than this probably

        # require extras
        ['experiment_date', TEMP.protocolExecutionDate],  # FIXME needs to reference a protocol
        ['injection_date', TEMP.protocolExecutionDate],  # FIXME needs to reference a protocol
        ['date_of_euthanasia', TEMP.protocolExecutionDate],
        ['date_of_injection', TEMP.protocolExecutionDate],
    ] + file_related + performance_related + utility
SubjectConverter.setup()


class SampleConverter(TripleConverter):
    mapping = [
        ['sample_id', TEMP.localId],  # unmangled
        #['subject_id', TEMP.wasDerivedFromSubject],
        ['specimen_anatomical_location', TEMPRAW.wasExtractedFromAnatomicalRegion],
        ['sample_name', rdfs.label],
        ['sample_description', dc.description],
        ['experimental_group', TEMP.hasAssignedGroup],
        ['group', TEMP.hasAssignedGroup],

        ['specimen', TEMP.TODO],  # sometimes used as anatomical location ...
        ['tissue', TEMP.TODO],
        ['counterstain', TEMP.TODO],
        ['muscle_layer', TEMP.TODO],

        # complex conversions required here ...
        ['target_electrode_location_relative_to_greater_lesser_curvature', TEMP.TODO],  # YEEEE aspects!
        ['target_electrode_orientation_relative_to_local_greater_curvature', TEMP.TODO],  # :D
        ['electrode_implant_location', TEMP.TODO],
        ['stimulation_electrode___chronic_or_acute', TEMP.TODO],
        ['includes_chronically_implanted_electrode_', TEMP.TODO],
    ] + file_related + performance_related + utility
    def subject_id(self, value):
        # FIXME _subject_id is monkey patched in
        return TEMP.wasDerivedFromSubject, self._subject_id(value)
SampleConverter.setup()


class ApiNATOMYConverter(TripleConverter):
    @staticmethod
    def apinatbase():
        # TODO move it external file
        yield TEMP.isAdvectivelyConnectedTo, rdf.type, owl.ObjectProperty
        yield TEMP.isAdvectivelyConnectedTo, rdf.type, owl.SymmetricProperty
        yield TEMP.isAdvectivelyConnectedTo, rdf.type, owl.TransitiveProperty

        yield TEMP.advectivelyConnects, rdf.type, owl.ObjectProperty
        yield TEMP.advectivelyConnects, rdf.type, owl.TransitiveProperty

        yield TEMP.advectivelyConnectsFrom, rdf.type, owl.ObjectProperty
        yield TEMP.advectivelyConnectsFrom, rdfs.subPropertyOf, TEMP.advectivelyConnects

        yield TEMP.advectivelyConnectsTo, rdf.type, owl.ObjectProperty
        yield TEMP.advectivelyConnectsTo, rdfs.subPropertyOf, TEMP.advectivelyConnects
        yield TEMP.advectivelyConnectsTo, owl.inverseOf, TEMP.advectivelyConnectsFrom

        idct = TEMP.isDiffusivelyConnectedTo
        yield idct, rdf.type, owl.ObjectProperty
        yield idct, rdf.type, owl.SymmetricProperty
        yield idct, rdf.type, owl.TransitiveProperty  # technically correct modulate the concentration gradient

        yield TEMP.diffusivelyConnects, rdf.type, owl.ObjectProperty
        yield TEMP.diffusivelyConnects, rdf.type, owl.TransitiveProperty

        yield TEMP.diffusivelyConnectsFrom, rdf.type, owl.ObjectProperty
        yield TEMP.diffusivelyConnectsFrom, rdfs.subPropertyOf, TEMP.advectivelyConnects

        yield TEMP.diffusivelyConnectsTo, rdf.type, owl.ObjectProperty
        yield TEMP.diffusivelyConnectsTo, rdfs.subPropertyOf, TEMP.advectivelyConnects
        yield TEMP.diffusivelyConnectsTo, owl.inverseOf, TEMP.advectivelyConnectsFrom

    def materialTriples(self, subject, link, predicate):
        rm = self._source

        def yield_from_id(s, matid, predicate=predicate):
            mat = rm[matid]
            if 'external' in mat:
                mat_s = OntTerm(mat['external'][0])
                yield s, predicate, mat_s.u
                yield mat_s.u, rdf.type, owl.Class
                yield mat_s.u, rdfs.label, rdflib.Literal(mat_s.label)
                if 'materials' in mat:
                    for submat_id in mat['materials']:
                        yield from yield_from_id(mat_s, submat_id, TEMP.hasConstituent)

            else:
                log.warning(f'no external id for {mat}')

        for matid in link['conveyingMaterials']:
            yield from yield_from_id(subject, matid)

    @property
    def triples_gen(self):
        rm = self._source

        # FIXME there doesn't seem to be a section that tells me the name
        # of top level model so I have to know its name beforhand
        # the id is in the model, having the id in the resource map
        # prevents issues if these things get sent decoupled
        id = rm['id']
        mid = id.replace(' ', '-')

        links = rm['resources'][id]['links']
        #linknodes = [n for n in rm[id]['nodes'] if n['class'] == 'Link']  # visible confusion

        st = []
        from_to = []
        ot = None
        yield from self.apinatbase()
        for link in links:
            if 'conveyingType' in link:
                if link['conveyingType'] == 'ADVECTIVE':
                    p_is =   TEMP.isAdvectivelyConnectedTo
                    p_from = TEMP.advectivelyConnectsFrom
                    p_to =   TEMP.advectivelyConnectsTo
                    p_cmat = TEMP.advectivelyConnectsMaterial
                    diffusive = False
                elif link['conveyingType'] == 'DIFFUSIVE':
                    p_is =   TEMP.isDiffusivelyConnectedTo
                    p_from = TEMP.diffusivelyConnectsFrom
                    p_to =   TEMP.diffusivelyConnectsTo
                    p_cmat = TEMP.diffusivelyConnectsMaterial
                    diffusive = True
                else:
                    log.critical(f'unhandled conveying type {link}')
                    continue

                source = link['source']
                target = link['target']
                ok = True
                if len(from_to) == 2:  # otherwise
                    st = []
                    from_to = []
                for i, e in enumerate((source, target)):
                    ed = rm[e]
                    if 'external' not in ed:
                        if not i and from_to:
                            # TODO make sure the intermediate ids match
                            pass
                        else:
                            ok = False
                            break
                    else:
                        st.append(e)
                        from_to.append(OntId(ed['external'][0]))

                conveying = link['conveyingLyph']
                cd = rm[conveying]
                if 'external' in cd:
                    old_ot = ot
                    ot = OntTerm(cd['external'][0])
                    yield ot.u, rdf.type, owl.Class
                    yield ot.u, TEMP.internalId, rdflib.Literal(conveying)
                    yield ot.u, rdfs.label, rdflib.Literal(ot.label)

                    yield from self.materialTriples(ot.u, link, p_cmat)  # FIXME locate this correctly

                    if ok:
                        u, d = from_to
                        if st[0] == source:
                            yield u, rdfs.label, rdflib.Literal(OntTerm(u).label)
                            yield u, rdf.type, owl.Class
                            yield from cmb.restriction.serialize(ot.u, p_from, u)

                        if st[1] == target:
                            yield d, rdfs.label, rdflib.Literal(OntTerm(d).label)
                            yield d, rdf.type, owl.Class
                            yield from cmb.restriction.serialize(ot.u, p_to, d)

                    if old_ot is not None and old_ot != ot:
                        yield from cmb.restriction.serialize(ot.u, p_from, old_ot.u)

                if diffusive:
                    # we can try to hack this using named individuals
                    # but it is not going to do exactly what is desired
                    s_link = TEMP[f'ApiNATOMY/{mid}/{link["id"]}']
                    s_cd = TEMP[f'ApiNATOMY/{mid}/{cd["id"]}']
                    yield s_link, rdf.type, owl.NamedIndividual
                    yield s_link, rdf.type, TEMP.diffusiveLink  # FIXME I'm not sure these go in the model ...
                    yield s_cd, rdf.type, owl.NamedIndividual
                    if 'external' in cd and cd['external']:
                        oid = OntId(cd['external'][0])
                        yield s_cd, rdf.type, oid.u
                        ot = oid.asTerm()
                        if ot.label:
                            yield oid.u, rdfs.label, ot.label

                    else:
                        yield s_cd, rdf.type, TEMP.conveyingLyph
                        for icd in cd['inCoalescences']:
                            dcd = rm[icd]
                            log.info(lj(dcd))
                            s_icd = TEMP[f'ApiNATOMY/{mid}/{dcd["id"]}']
                            yield s_cd, TEMP.partOfCoalescence, s_icd
                            yield s_icd, rdf.type, owl.NamedIndividual
                            yield s_icd, rdf.type, TEMP['ApiNATOMY/Coalescence']
                            if 'external' in dcd and dcd['external']:
                                oid = OntId(dcd['external'][0])
                                yield s_icd, rdf.type, oid.u
                                ot = oid.asTerm()
                                if ot.label:
                                    yield oid.u, rdfs.label, ot.label

                            for lyphid in dcd['lyphs']:
                                ild = rm[lyphid]
                                log.info(lj(ild))
                                if 'external' in ild and ild['external']:
                                    yield s_icd, TEMP.hasLyphWithMaterial, OntId(ild['external'][0])

                if not ok:
                    logd.info(f'{source} {target} issue')
                    continue

                for inid, e in zip(st, from_to):
                    yield e.u, rdf.type, owl.Class
                    yield e.u, rdfs.label, rdflib.Literal(OntTerm(e).label)
                    yield e.u, TEMP.internalId, rdflib.Literal(inid)

                f, t = from_to
                yield from cmb.restriction.serialize(f.u, p_is, t.u)

ApiNATOMYConverter.setup()
